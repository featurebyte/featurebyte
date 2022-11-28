As a preventative measure, data quality annotation at the data source level is supported and strongly encouraged as well as the setting of default cleaning steps.

During the annotation, users can define how to handle:

* missing values,
* disguised values,
* values not in an expected list,
* out of boundaries numeric values and dates,
* and string values when numeric values or dates are expected.

Users can define data pipeline data cleaning settings to either ignore values with quality issues or impute those values.

Those default cleaning steps are then automatically enforced if no cleaning steps are specified during feature declaration.
