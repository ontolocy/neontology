"""Node models shared by more than one test_basenode_* module.

Primary labels must be unique across the suite (type discovery is global), so a model
used by several modules is defined once here rather than redefined.
"""

from enum import Enum
from typing import ClassVar, Optional

from neontology import BaseNode, GQLIdentifier


class PracticeNode(BaseNode):
    __primaryproperty__: ClassVar[GQLIdentifier] = "pp"
    __primarylabel__: ClassVar[Optional[GQLIdentifier]] = "PracticeNode"

    pp: str


class SampleEnum(str, Enum):
    VALUE1 = "value1"
    VALUE2 = "value2"
    VALUE3 = "value3"
