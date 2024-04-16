from isb_lib.utilities.enums import _NoValue


class ISBFormat(_NoValue):
    """Format parameter for JSON records"""

    ORIGINAL = "original"
    CORE = "core"
    FULL = "full"
    SOLR = "solr"


class ISBAuthority(_NoValue):
    """Format parameter for known iSB authorities"""

    GEOME = "geome"
    OPENCONTEXT = "opencontext"
    SESAR = "sesar"
    SMITHSONIAN = "smithsonian"


class ISBReturnField(_NoValue):
    """Return field parameter for interactive debugging"""

    CONTEXT = "context"
    MATERIAL = "material"
    SPECIMEN = "specimen"
    ALL = "all"

    def dictionary_key(self) -> str:
        return f"has{self.value.capitalize()}Category"
