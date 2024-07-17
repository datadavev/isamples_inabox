import typing

from isamples_metadata.Transformer import Transformer


class CoreJSONTransformer(Transformer):

    def transform(self, include_h3: bool = True) -> typing.Dict:
        return self.source_record
