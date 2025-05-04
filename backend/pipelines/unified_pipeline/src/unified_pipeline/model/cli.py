from enum import Enum

from pydantic import BaseModel


class Env(Enum):
    local = "local"
    dev = "dev"
    prod = "prod"


class Source(Enum):
    bnbo = "bnbo"


class Stage(Enum):
    bronze = "bronze"
    silver = "silver"
    all = "all"


class CliConfig(BaseModel):
    env: Env = Env.prod
    source: Source
    stage: Stage
