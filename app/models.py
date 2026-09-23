from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    event_id: str = Field(min_length=1, max_length=200)
    event_type: str
    order_id: str = Field(min_length=1, max_length=200)
    customer_ref: str = Field(min_length=1, max_length=200)
    amount_cents: int = Field(ge=0)
    currency: str

    @field_validator("event_type")
    @classmethod
    def supported_type(cls, value: str) -> str:
        if value != "order.created":
            raise ValueError("unsupported event_type")
        return value

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        value = value.strip().upper()
        if len(value) != 3 or not value.isalpha():
            raise ValueError("currency must be a three-letter code")
        return value


class Outcome(StrEnum):
    SUCCESS = "SUCCESS"
    RETRYABLE = "RETRYABLE"
    TERMINAL = "TERMINAL"
    CRASH = "CRASH"


class DownstreamResult(BaseModel):
    outcome: Outcome
    status_code: int | None = None
    error_code: str | None = None
    error_class: str | None = None
