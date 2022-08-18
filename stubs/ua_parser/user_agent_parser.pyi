from typing import Any, Optional, TypedDict

class UserAgent(TypedDict):
    family: str
    major: Optional[str]
    minor: Optional[str]
    patch: Optional[str]

class OperatingSystem(TypedDict):
    family: str
    major: Optional[str]
    minor: Optional[str]
    patch: Optional[str]
    patch_minor: Optional[str]

class Device(TypedDict):
    family: str
    brand: Optional[str]
    model: Optional[str]

class UAInfo(TypedDict):
    user_agent: UserAgent
    os: OperatingSystem
    device: Device
    string: str

def Parse(user_agent_string: str, **kwargs: Any) -> UAInfo: ...
