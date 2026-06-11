"""
Unified configuration for KVStream.

Priority order (highest to lowest):
  1. Constructor / direct attribute assignment (CLI args)
  2. Environment variables  KVSTREAM_BACKEND__TYPE=foundry
  3. kvault.yaml in the working directory
  4. Built-in defaults
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Optional, Tuple, Type

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class BackendType(str, Enum):
    OLLAMA   = "ollama"
    FOUNDRY  = "foundry"
    LLAMACPP = "llamacpp"
    LMSTUDIO = "lmstudio"


class AttentionBackend(str, Enum):
    FLASH    = "flash"
    XFORMERS = "xformers"
    NAIVE    = "naive"


class PreemptionPolicy(str, Enum):
    SWAP      = "swap"
    RECOMPUTE = "recompute"


class SchedulerPolicy(str, Enum):
    FCFS = "fcfs"
    SJF  = "sjf"


# ---------------------------------------------------------------------------
# Sub-configs  (plain dataclass-style, NOT BaseSettings — avoids env conflicts)
# ---------------------------------------------------------------------------

from pydantic import BaseModel


class BackendConfig(BaseModel):
    type:               BackendType = BackendType.FOUNDRY
    base_url:           str         = "http://localhost:5273"
    model:              str         = "phi-3-mini"
    timeout_seconds:    float       = 120.0
    kv_inject_enabled:  bool        = True
    kv_cache_dir:       str         = "/tmp/kvstream_kv_cache"


class MemoryConfig(BaseModel):
    num_gpu_blocks: int                                     = 256
    num_cpu_blocks: int                                     = 512
    block_size:     int                                     = 16
    dtype:          Literal["float16","bfloat16","float32"] = "float16"

    @field_validator("block_size")
    @classmethod
    def must_be_power_of_two(cls, v: int) -> int:
        if v & (v - 1) != 0:
            raise ValueError("block_size must be a power of two (8, 16, 32 …)")
        return v


class SchedulerConfig(BaseModel):
    max_batch_size:            int               = 8
    max_waiting_tokens:        int               = 4096
    preemption_policy:         PreemptionPolicy  = PreemptionPolicy.SWAP
    priority:                  SchedulerPolicy   = SchedulerPolicy.FCFS
    max_tokens_per_seq:        int               = 8192
    # How long a request may queue for batch admission before it is rejected.
    admission_timeout_seconds: float             = 120.0


class PrefixCacheConfig(BaseModel):
    enabled:            bool = True
    max_prefix_length:  int  = 2048
    ttl_seconds:        int  = 3600
    min_match_tokens:   int  = 16


class AttentionConfig(BaseModel):
    backend:    AttentionBackend = AttentionBackend.NAIVE
    num_heads:  int              = 32
    head_dim:   int              = 128
    num_layers: int              = 32


class ObservabilityConfig(BaseModel):
    metrics_enabled: bool                               = True
    metrics_port:    int                                = 9090
    log_level:       Literal["DEBUG","INFO","WARNING","ERROR"] = "INFO"
    trace_requests:  bool                               = False


# ---------------------------------------------------------------------------
# Top-level config  (BaseSettings for env-var support)
# ---------------------------------------------------------------------------

class KVaultConfig(BaseSettings):
    """
    Top-level config.  Reads from (in priority order):
      1. Direct assignment after construction  (CLI always wins)
      2. Env vars:  KVSTREAM_BACKEND__TYPE=foundry
      3. kvault.yaml in cwd
      4. Defaults above
    """
    model_config = SettingsConfigDict(
        env_prefix            = "KVSTREAM_",
        env_nested_delimiter  = "__",
        # yaml_file removed intentionally — we load YAML manually below
        # to avoid the pydantic-settings v2 source-wiring requirement
    )

    # Bind to loopback by default — the proxy has no authentication layer.
    # Set host: 0.0.0.0 explicitly (or --host) to expose it on the network.
    host:         str  = "127.0.0.1"
    port:         int  = 8080

    backend:       BackendConfig       = Field(default_factory=BackendConfig)
    memory:        MemoryConfig        = Field(default_factory=MemoryConfig)
    scheduler:     SchedulerConfig     = Field(default_factory=SchedulerConfig)
    prefix_cache:  PrefixCacheConfig   = Field(default_factory=PrefixCacheConfig)
    attention:     AttentionConfig     = Field(default_factory=AttentionConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    @classmethod
    def from_yaml(cls, path: str) -> "KVaultConfig":
        """
        Load from a YAML file. Fields set in the file take precedence;
        env vars (KVSTREAM_*) fill in any fields the file omits.
        """
        import yaml
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        # Passing the YAML data as init kwargs lets pydantic-settings layer
        # the remaining fields from environment variables and defaults.
        return cls(**data)

    @classmethod
    def auto(cls, config_file: Optional[str] = None) -> "KVaultConfig":
        """
        Load config automatically:
          - Use config_file if provided
          - Otherwise try kvault.yaml in cwd
          - Otherwise use defaults + env vars
        """
        import os
        if config_file:
            return cls.from_yaml(config_file)
        if os.path.exists("kvault.yaml"):
            return cls.from_yaml("kvault.yaml")
        return cls()
