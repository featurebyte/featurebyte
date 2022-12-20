As a preventative measure, data quality annotation at the data source level, and data cleaning step declarations, are supported and strongly encouraged.

During the annotation, users can define how to handle:

* missing values,
* disguised values,
* values not in an expected list,
* out of boundaries numeric values and dates,
* and string values when numeric values or dates are expected.

Users can define data pipeline data cleaning settings to either ignore values with quality issues when aggregations are performed or impute those values.

Those default cleaning steps are then automatically enforced if no cleaning steps are explicitly specified during feature declaration.
