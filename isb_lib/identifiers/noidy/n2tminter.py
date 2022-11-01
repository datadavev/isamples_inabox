"""
Implements stateful identifier minting

This implementation borrows heavily from the EZID minter implementation. The
main change is persistence of state in a plain python dict instead of
using Berkeley DB.

See https://github.com/CDLUC3/ezid for original implementation.
See https://github.com/datadavev/noidy for original python port.

Minted identifiers are short strings generated in sequence
for a given state.

This minter must be used in a singleton pattern (one, and only one,
instance per state) otherwise collisions will occur.
"""

import typing
import logging
import re
from . import Minter

# fmt:off
XDIG_DICT = {
    # digits
    "0": 0, "1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9,
    # chars
    "b": 10, "c": 11, "d": 12, "f": 13, "g": 14, "h": 15, "j": 16, "k": 17, "m": 18,
    "n": 19, "p": 20, "q": 21, "r": 22, "s": 23, "t": 24, "v": 25, "w": 26, "x": 27,
    "z": 28,
}
# fmt:on
XDIG_STR = "0123456789bcdfghjkmnpqrstvwxz"
ALPHA_COUNT = len(XDIG_STR)
DIGIT_COUNT = 10

L = logging.getLogger(__name__)


class _Drand48:
    """48-bit linear congruential PRNG, matching srand48() and drand48() in
    glibc.

    The sequence of pseudo-random numbers generated by this PRNG matches
    that of N2T Nog running on Perl, when Perl is built with GCC on
    Linux.
    """

    def __init__(self, seed):
        self.state = (seed << 16) + 0x330E

    def drand(self) -> int:
        self.state = (25214903917 * self.state + 11) & (2 ** 48 - 1)
        rnd = self.state / 2 ** 48
        return rnd


class N2TMinter(Minter):
    """ Implements a stateful minter using the same algorithm as N2T and EZID.
    """
    def __init__(self, shoulder_str: str, mask_str: str = "eedk"):
        """

        Args:
            shoulder_str:
            mask_str:
        """
        super().__init__()
        self.name = "N2tMinter"
        self.active_counter_list = None
        self.inactive_counter_list = None
        self.counter_list = []
        self.template_str = "{}{{}}".format(shoulder_str, mask_str)
        self.mask_str = mask_str
        self.original_template = self.template_str
        self.origmask = mask_str

        self.base_count = 0
        self.combined_count = 0
        self.max_combined_count = 0
        self.total_count = 0
        self.max_per_counter = 0
        self.atlast_str = "add0"
        self._extend_template()
        self.atlast_str = "add3"
        self.shoulder = shoulder_str

    def asDict(self) -> dict:
        """ Presents state of self as a dict.

        Returns:
            dict with state information in similar format to N2T and EZID
        """
        d = {
            "name": self.name,
            "basecount": self.base_count,
            "oacounter": self.combined_count,
            "oatop": self.max_combined_count,
            "total": self.total_count,
            "percounter": self.max_per_counter,
            "template": self.template_str,
            "mask": self.mask_str,
            "atlast": self.atlast_str,
            "saclist": self.active_counter_list,
            "siclist": self.inactive_counter_list,
            "original_template": self.original_template,
            "original_mask": self.origmask,
        }
        for n, (top, value) in enumerate(self.counter_list):
            d[f'c{n}'] = {
                'top': top,
                'value': value,
            }
        return d

    def fromDict(self, d: dict) -> None:
        """ Loads state of self from provided dict.

        Args:
            d: Dict of same structure as produced by .asDict()

        Returns:
            Nothing
        """
        self.name = d.get("name", self.name)
        self.base_count = d.get("basecount")
        self.combined_count = d.get("oacounter")
        self.max_combined_count = d.get("oatop")
        self.total_count = d.get("total")
        self.max_per_counter = d.get("percounter")
        self.template_str = d.get("template")
        self.mask_str = d.get("mask")
        self.atlast_str = d.get("atlast")
        self.active_counter_list = d.get("saclist")
        self.inactive_counter_list = d.get("siclist")
        self.counter_list = []
        i = 0
        while True:
            try:
                self.counter_list.append(
                    (
                        d[f'c{i}']['top'],
                        d[f'c{i}']['value']
                    )
                )
            except KeyError:
                break
            i += 1

    def mint(self, count: int = 1) -> typing.Generator[str, None, None]:
        """Yield one or more identifiers.

        The generated strings are unique for the sequence as represented
        by the state of self. Repeated calls from the same starting state
        will yield the same strings. Hence the ending state must be
        used for a subsequent call.

        Args:
            count (int): Number of identifiers to yield.
        """
        self._assert_ezid_compatible_minter()
        self._assert_valid_combined_count()
        self._assert_mask_matches_template()
        fmt_str = re.sub("{.*}", "{}", self.template_str)
        for _ in range(count):
            if self.combined_count == self.max_combined_count:
                self._extend_template()
            compounded_counter = self._next_state()
            self.combined_count += 1
            xdig_str = self._get_xdig_str(compounded_counter)
            if self.mask_str.endswith("k"):
                minted_id = fmt_str.format(xdig_str)
                xdig_str += self._get_check_char(minted_id)
            yield xdig_str

    # -- Internal methods --

    def _next_state(self):
        """Step the minter to the next state."""
        rnd = _Drand48(self.combined_count)
        active_counter_idx = int(rnd.drand() * len(self.active_counter_list))
        # L.debug(
        #     'len(self.active_counter_list)={}'.format(len(self.active_counter_list))
        # )
        L.debug("active_counter_idx=%s", active_counter_idx)
        counter_name = self.active_counter_list[active_counter_idx]
        counter_idx = int(counter_name[1:])
        max_int, value_int = self.counter_list[counter_idx]
        value_int += 1
        self.counter_list[counter_idx] = max_int, value_int
        n = value_int + counter_idx * self.max_per_counter
        if value_int >= max_int:
            self._deactivate_exhausted_counter(active_counter_idx)
        return n

    def _get_xdig_str(self, compounded_counter):
        """Convert compounded counter value to final sping as specified by the
        mask."""
        s = []
        for c in reversed(self.mask_str):
            if c == "k":
                continue
            elif c in ("e", "f"):
                divider = ALPHA_COUNT
            elif c == "d":
                divider = DIGIT_COUNT
            else:
                raise ValueError(
                    "Unsupported character in mask: {}".format(c)
                )
            compounded_counter, rem = divmod(compounded_counter, divider)
            x_char = XDIG_STR[rem]
            if c == "f" and x_char.isdigit():
                return ""
            s.append(x_char)
        return "".join(reversed(s))

    def _get_check_char(self, id_str):
        total_int = 0
        for i, c in enumerate(id_str):
            total_int += (i + 1) * XDIG_DICT.get(c, 0)
        return XDIG_STR[total_int % ALPHA_COUNT]

    def _extend_template(self):
        """Called when the minter has been used for minting the maximum number
        of IDs that is possible using the current mask (combined_count has
        reached max_combined_count).

        In order to use the minter again, the mask must be extended to
        accommodate longer IDs. This affects many of the values in the
        minter, which have to be recalculated based on the new mask.
        """
        self._assert_exhausted_minter()
        self._transfer_to_base_count()
        self._extend_mask()
        self._set_new_max_counts()
        self._reset_inactive_counter_list()
        self._generate_active_counter_list()

    def _generate_active_counter_list(self):
        """Generate new list of active counters and their top values after all
        counters have been exhausted."""
        # The total number of possible identifiers for a given mask is divided by this
        # number in order to get the max value per counter. All counters have the same
        # max value except for (usually) the last one, which receives the reminder.
        #
        # Comment about this value from N2T Nog:
        #
        # prime, a little more than 29*10. Using a prime under the theory (unverified)
        # that it may help even out distribution across the more significant digits of
        # generated strings.  In this way, for example, a method for mapping an string
        # to a pathname (eg, fk9tmb35x -> fk/9t/mb/35/x/, which could be a directory
        # holding all files related to the named object), would result in a reasonably
        # balanced filesystem tree -- no subdirectories too unevenly loaded. That's the
        # hope anyway.
        prime_factor = 293
        self.max_per_counter = int(self.total_count / prime_factor + 1)
        n = 0
        t = self.total_count
        self.counter_list = []
        self.active_counter_list = []
        while t > 0:
            self.counter_list.append(
                (self.max_per_counter if t >= self.max_per_counter else t, 0)
            )
            self.active_counter_list.append("c{}".format(n))
            t -= self.max_per_counter
            n += 1

    def _set_new_max_counts(self):
        """Calculate the number of identifiers that can be minted with the new
        mask.

        When this number is reached, the template must be extended
        again.
        """
        v = self._get_max_count()
        self.total_count = v
        self.max_combined_count = v

    def _extend_mask(self):
        """Extend the mask according to the "atlast" rule."""
        m = re.match(r"add(\d)$", self.atlast_str)
        add_int = int(m.group(1))
        self.mask_str = self.mask_str[:add_int] + self.mask_str
        # Insert the extended mask into the minter template.
        self.template_str = re.sub(
            r"{.*}", "{{{}}}".format(self.mask_str), self.template_str
        )

    def _transfer_to_base_count(self):
        """Capture combined_count by adding it to the base_count, then reset it
        back to zero.

        The total number of identifiers minted since the minter was
        created is always base_count + combined_count.
        """
        self.base_count += self.combined_count
        self.combined_count = 0

    def _deactivate_exhausted_counter(self, counter_idx):
        """Deactivate an exhausted counter by moving it from the active to the
        inactive counter list."""
        counter_name = self.active_counter_list.pop(counter_idx)
        self.inactive_counter_list.append(counter_name)

    def _reset_inactive_counter_list(self):
        """Clear list of exhausted counters."""
        self.inactive_counter_list = []

    def _assert_exhausted_minter(self):
        """Check that we really have an exhausted minter.

        An exhausted minter must have no remaining counters in the
        active list. All the counters should be in the inactive list.
        """
        if not (self.combined_count == self.max_combined_count == self.total_count):
            raise ValueError("Attempted to extend a minter that is not exhausted")
        if self.active_counter_list:
            raise ValueError(
                "Attempted to extend a minter that still has active counters"
            )

    def _assert_ezid_compatible_minter(self):
        """Ensure that we can handle this minter.

        EZID uses minters that require only a subset of the features
        available on N2T. This code handles more than the EZID subset
        but not the full N2T set.
        """
        if not re.match(r"[def]+k?$", self.mask_str):
            raise ValueError(
                "Mask must use only 'd', 'e' and 'f' character types, "
                "ending with optional 'k' check character: {}".format(self.mask_str)
            )

        if not re.match(r"add(\d)$", self.atlast_str):
            raise ValueError(
                '"atlast" must be a string on form: add<digit>: {}'.format(
                    self.atlast_str
                )
            )

    def _assert_valid_combined_count(self):
        if self.combined_count > self.max_combined_count:
            raise ValueError(
                "Invalid counter total sum. total={} max={}".format(
                    self.combined_count, self.max_combined_count
                )
            )

    def _assert_mask_matches_template(self):
        if self.template_str.find("{{{}}}".format(self.mask_str)) == -1:
            raise ValueError(
                "The mask that is embedded in the template key/value must match the "
                "mask that is stored separately in the mask key/value. "
                'template="{}" mask="{}"'.format(self.template_str, self.mask_str)
            )

    def _get_max_count(self):
        """Calculate the max number of spings that can be generated with a
        given mask."""
        max_count = 1
        for c in self.mask_str:
            if c == "k":
                continue
            elif c in ("e", "f"):
                max_count *= ALPHA_COUNT
            elif c == "d":
                max_count *= DIGIT_COUNT
            else:
                raise ValueError("Unsupported character in mask: {}".format(c))
        return max_count
