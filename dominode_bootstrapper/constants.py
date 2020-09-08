import enum


# TODO: Remove this
class DepartmentName(enum.Enum):
    PPD = 'ppd'
    LSD = 'lsd'


class UserRole(str, enum.Enum):
    REGULAR_DEPARTMENT_USER = 'regular_department_user'
    EDITOR = 'editor'


class GeofenceAccess(enum.Enum):
    ADMIN = enum.auto()
    USER = enum.auto()