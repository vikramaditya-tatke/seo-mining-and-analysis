from polars import Expr
from pydantic import BaseModel, ConfigDict, Field


class DataField(BaseModel):
    """Defines a mapping from a nested JSON path to a flat column alias.

    Attributes:
        alias: The name of the output column.
        path: A list of keys representing the path to the nested field.
    """

    alias: str = Field(..., min_length=1)
    path: list[str] = Field(..., min_length=1)
    model_config = ConfigDict(frozen=True, extra="forbid")

    def to_polars_expr(self, root: Expr) -> Expr:
        """Generates the nested struct.field expression dynamically.

        Args:
            root: The root Polars expression to start the extraction from.

        Returns:
            A Polars expression that extracts the nested field and aliases it.
        """
        expr = root
        for segment in self.path:
            expr = expr.struct.field(segment)
        return expr.alias(self.alias)
