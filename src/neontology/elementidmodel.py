from typing import ClassVar, Optional

from pydantic import BaseModel, Field


class ElementIdModel(BaseModel):
    __elementidproperty__: ClassVar[str] = "element_id"
    element_id: Optional[str] = Field("", json_schema_extra={"never_set": True})
