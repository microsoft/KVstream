"""
What models Foundry Local can serve right now — downloaded versus loaded.

Two facts, measured on Foundry Local CLI 0.10.3, shape this module:

* ``/v1/models`` lists every **downloaded** model, loaded or not. A request
  naming a downloaded-but-unloaded model is refused ("not loaded"), and a
  *streamed* one is answered ``200 text/event-stream`` followed by a dropped
  connection — which looks exactly like a crash.
* Only ``foundry cache list --output json`` says what is **loaded**, and only
  where the CLI is usable (not inside a container).

So the inventory keeps both lists and records when one of them is unknown,
rather than treating "listed" as "loaded". Everything that decides from it
(auto-detecting the model, refusing a stream that cannot succeed, startup
warnings) is a pure function of a :class:`Snapshot`, so it can be tested
without a backend.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass

logger = logging.getLogger("kvstream.inventory")


@dataclass(frozen=True)
class Snapshot:
    """One reading of the backend's models. ``None`` means "could not tell"."""

    downloaded: frozenset[str] | None = None  # ids from /v1/models
    loaded: frozenset[str] | None = None  # ids with loaded=true, from the CLI
    taken_at: float = 0.0
    # {id: type} from the CLI (`"Chat"` for a chat model on CLI 0.10.3).
    types: Mapping[str, str] | None = None

    def as_dict(self) -> dict:
        return {
            "downloaded": sorted(self.downloaded) if self.downloaded is not None else None,
            "loaded": sorted(self.loaded) if self.loaded is not None else None,
            "age_seconds": round(time.monotonic() - self.taken_at, 1) if self.taken_at else None,
            "types": dict(sorted(self.types.items())) if self.types is not None else None,
        }


@dataclass(frozen=True)
class Selection:
    """The outcome of auto-detecting which model KVStream should use."""

    model: str | None
    detail: str


@dataclass(frozen=True)
class Refusal:
    """Why a request for a model cannot succeed, as an OpenAI error code + message."""

    code: str
    message: str


# ----------------------------------------------------------------------
# Matching
# ----------------------------------------------------------------------


def unversioned(model_id: str) -> str:
    """Drop a numeric ``:N`` catalog version suffix (``…-gpu:2`` → ``…-gpu``)."""
    base, sep, suffix = model_id.rpartition(":")
    return base if sep and suffix.isdigit() else model_id


def matches(model: str, ids: frozenset[str] | set[str] | list[str]) -> bool:
    """
    Whether ``model`` names one of ``ids``.

    Foundry Local lists a model without its version suffix in ``/v1/models``,
    with it in the CLI, and accepts it either way — so the suffix is ignored on
    both sides.
    """
    return unversioned(model) in {unversioned(i) for i in ids}


# ----------------------------------------------------------------------
# Decisions (pure)
# ----------------------------------------------------------------------


def _listing(ids: frozenset[str]) -> str:
    return ", ".join(sorted(ids)) or "none"


def select_model(snap: Snapshot) -> Selection:
    """
    Pick the model to use when none was configured — or say why not.

    Only an unambiguous answer is accepted. With exactly one model loaded, that
    is the model. With none or several, there is nothing to infer, and guessing
    would silently probe and calibrate against the wrong model.
    """
    if snap.loaded is not None:
        if len(snap.loaded) == 1:
            (model,) = snap.loaded
            return Selection(model, "the only model loaded in Foundry Local")
        if not snap.loaded:
            downloaded = f" Downloaded: {_listing(snap.downloaded)}." if snap.downloaded else ""
            return Selection(
                None,
                "no model is loaded in Foundry Local."
                + downloaded
                + " Load one with `foundry model load <alias>`, or pass --model.",
            )
        return Selection(
            None,
            f"{len(snap.loaded)} models are loaded in Foundry Local ({_listing(snap.loaded)}); "
            "pass --model to choose which one KVStream health-checks and calibrates against.",
        )
    # The loaded list is unknown (no usable CLI, e.g. in a container). The API
    # list is only downloaded models, so a single entry is the only candidate,
    # not a confirmed load — the readiness probe confirms it or reports why not.
    if snap.downloaded is not None:
        if len(snap.downloaded) == 1:
            (model,) = snap.downloaded
            return Selection(
                model,
                "the only model Foundry Local lists; whether it is loaded could not be "
                "checked (no usable foundry CLI), so the readiness probe will confirm it",
            )
        if not snap.downloaded:
            return Selection(None, "Foundry Local lists no models. Download and load one first.")
        return Selection(
            None,
            f"Foundry Local lists {len(snap.downloaded)} models ({_listing(snap.downloaded)}) "
            "and which are loaded could not be checked; pass --model.",
        )
    return Selection(None, "could not read Foundry Local's model list (is it running?)")


def refusal_for(model: str, snap: Snapshot) -> Refusal | None:
    """
    Why a request for ``model`` is known to fail, or ``None`` to let it through.

    Used for streamed requests, which Foundry answers with a dropped connection
    instead of an error when the model is unknown or not loaded. Fails open:
    whatever cannot be read is not held against the request.
    """
    if snap.downloaded and not matches(model, snap.downloaded):
        return Refusal(
            "model_not_found",
            f"model {model!r} is not available in Foundry Local, which lists: "
            f"{_listing(snap.downloaded)}. Use one of those ids. A catalog alias such as "
            "'phi-3-mini-4k' is not accepted by the inference API.",
        )
    if snap.loaded is not None and not matches(model, snap.loaded):
        return Refusal(
            "model_not_loaded",
            f"model {model!r} is downloaded but not loaded in Foundry Local "
            f"(loaded: {_listing(snap.loaded)}). Load it with `foundry model load`, "
            "then retry.",
        )
    return None


# Model types that must not be sent an embeddings request. Only what has been
# observed: on Foundry Local CLI 0.10.3 an embeddings request naming a *chat*
# model was followed both times by the service dying (a dropped connection and a
# restart on a new port; then a 30 s hang, a reset for an unrelated chat request,
# and no daemon). Other types are not refused on a guess.
NOT_EMBEDDING_TYPES = frozenset({"chat"})


def embedding_refusal(model: str, snap: Snapshot) -> Refusal | None:
    """
    Why an embeddings request for ``model`` must not be forwarded, or ``None``.

    Fails open: a model whose type cannot be read — no CLI, a container, a
    model Foundry does not list — is let through, exactly as before.
    """
    if not snap.types:
        return None
    wanted = unversioned(model)
    for model_id, kind in snap.types.items():
        if unversioned(model_id) == wanted and kind.lower() in NOT_EMBEDDING_TYPES:
            return Refusal(
                "model_not_embedding",
                f"model {model!r} is a {kind} model in Foundry Local, not an embedding "
                "model, so KVStream does not forward embeddings requests for it: on "
                "Foundry Local 0.10.3 such a request was observed to stop the service "
                "for every client. Use an embedding model.",
            )
    return None


def diagnose(model: str, snap: Snapshot, catalog: list[str] | None) -> str | None:
    """
    A startup warning for a configured ``model`` that will be refused, or ``None``.

    The backend's own lists decide; the catalog (``foundry model list``) only
    makes the message precise, or catches typos when the lists are unreadable.
    """
    refusal = refusal_for(model, snap)
    if refusal is None:
        if snap.downloaded is None and catalog and not matches(model, catalog):
            shown = ", ".join(catalog[:8]) + ("…" if len(catalog) > 8 else "")
            return (
                f"configured model {model!r} is not in Foundry Local's catalog ({shown}). "
                "Set --model to one of those, and load it with `foundry model load`."
            )
        return None
    if refusal.code == "model_not_found" and catalog and matches(model, catalog):
        return (
            f"configured model {model!r} is in Foundry Local's catalog but is not an id the "
            f"inference API accepts; it lists: {_listing(snap.downloaded or frozenset())}. "
            f"If {model!r} is a catalog alias, use the full id instead."
        )
    return f"configured {refusal.message}"


# ----------------------------------------------------------------------
# The cached reader
# ----------------------------------------------------------------------


class ModelInventory:
    """
    A cached :class:`Snapshot`, cheap enough to consult on the request path.

    Each reading costs roughly 100 ms on the measured machine (an HTTP call and
    a CLI subprocess). A stale snapshot is therefore refreshed in the
    background while the old one keeps answering; only a request for a model
    the snapshot does not know waits for a fresh reading, and such forced
    readings are limited to one per ``min_refresh_seconds`` so a flood of bad
    requests cannot become a flood of subprocesses.
    """

    def __init__(
        self,
        read_downloaded: Callable[[], Awaitable[list[str] | None]],
        read_loaded: Callable[[], Awaitable[list[str] | None]],
        *,
        read_types: Callable[[], Awaitable[Mapping[str, str] | None]] | None = None,
        stale_seconds: float = 30.0,
        min_refresh_seconds: float = 1.0,
    ) -> None:
        self._read_downloaded = read_downloaded
        self._read_loaded = read_loaded
        # Called right after `read_loaded`, and only when that succeeded, so an
        # implementation may answer from the same CLI call.
        self._read_types = read_types
        self._stale = stale_seconds
        self._min_refresh = min_refresh_seconds
        self._snap = Snapshot()
        self._lock = asyncio.Lock()
        self._background: asyncio.Task | None = None

    @property
    def current(self) -> Snapshot:
        return self._snap

    async def snapshot(self, fresh: bool = False) -> Snapshot:
        """The current reading; ``fresh`` forces a new one (rate-limited)."""
        age = time.monotonic() - self._snap.taken_at
        if not self._snap.taken_at:
            return await self._refresh()
        if fresh and age >= self._min_refresh:
            return await self._refresh()
        if age >= self._stale and (self._background is None or self._background.done()):
            self._background = asyncio.create_task(self._refresh())
        return self._snap

    async def _refresh(self) -> Snapshot:
        async with self._lock:
            # Another caller may have refreshed while this one waited.
            if self._snap.taken_at and time.monotonic() - self._snap.taken_at < self._min_refresh:
                return self._snap
            downloaded = await self._read_downloaded()
            # The CLI's list is read only when the API answered: on CLI 0.10.3
            # `foundry cache list` starts a stopped daemon, and KVStream must
            # never start Foundry Local as a side effect of a health check.
            loaded = await self._read_loaded() if downloaded is not None else None
            types = (
                await self._read_types()
                if self._read_types is not None and loaded is not None
                else None
            )
            self._snap = Snapshot(
                downloaded=frozenset(downloaded) if downloaded is not None else None,
                loaded=frozenset(loaded) if loaded is not None else None,
                taken_at=time.monotonic(),
                types=dict(types) if types is not None else None,
            )
            return self._snap

    async def aclose(self) -> None:
        if self._background is not None and not self._background.done():
            self._background.cancel()
