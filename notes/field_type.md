
There are several `types` for each field (column) type, such as `type()`/`real_type()/binlog_type()` ...

By default, `real_type()` and `binlog_type()` are the same as `type()`:

```
  // From mysql-8.0/sql/field.h Field

  // ...
  virtual enum_field_types real_type() const { return type(); }
  virtual enum_field_types binlog_type() const {
    /*
      Binlog stores field->type() as type code by default.
      This puts MYSQL_TYPE_STRING in case of CHAR, VARCHAR, SET and ENUM,
      with extra data type details put into metadata.

      We cannot store field->type() in case of temporal types with
      fractional seconds: TIME(n), DATETIME(n) and TIMESTAMP(n),
      because binlog records with MYSQL_TYPE_TIME, MYSQL_TYPE_DATETIME
      type codes do not have metadata.
      So for temporal data types with fractional seconds we'll store
      real_type() type codes instead, i.e.
      MYSQL_TYPE_TIME2, MYSQL_TYPE_DATETIME2, MYSQL_TYPE_TIMESTAMP2,
      and put precision into metatada.

      Note: perhaps binlog should eventually be modified to store
      real_type() instead of type() for all column types.
    */
    return type();
  }
  // ...
```

Here is a list collected from `mysql-8.0/sql/field.h`:

```
+------------------------------------------------------------+-------------------------------+-----------------------+------------------------+
|                           Field                            |            type()             |      real_type()      |      binlog_type()     |
+------------------------------------------------------------+-------------------------------+-----------------------+------------------------+
|                                                            |                               |                       |                        |
| Field (abstract)                                           |                               |                       |                        |
| |                                                          |                               |                       |                        |
| +--Field_bit                                               | MYSQL_TYPE_BIT                |                       |                        |
| |  +--Field_bit_as_char                                    |                               |                       |                        |
| |                                                          |                               |                       |                        |
| +--Field_num (abstract)                                    |                               |                       |                        |
| |  |  +--Field_real (abstract)                             |                               |                       |                        |
| |  |     +--Field_decimal                                  | MYSQL_TYPE_DECIMAL            |                       |                        |
| |  |     +--Field_float                                    | MYSQL_TYPE_FLOAT              |                       |                        |
| |  |     +--Field_double                                   | MYSQL_TYPE_DOUBLE             |                       |                        |
| |  |                                                       |                               |                       |                        |
| |  +--Field_new_decimal                                    | MYSQL_TYPE_NEWDECIMAL         |                       |                        |
| |  +--Field_short                                          | MYSQL_TYPE_SHORT              |                       |                        |
| |  +--Field_medium                                         | MYSQL_TYPE_INT24              |                       |                        |
| |  +--Field_long                                           | MYSQL_TYPE_LONG               |                       |                        |
| |  +--Field_longlong                                       | MYSQL_TYPE_LONGLONG           |                       |                        |
| |  +--Field_tiny                                           | MYSQL_TYPE_TINY               |                       |                        |
| |     +--Field_year                                        | MYSQL_TYPE_YEAR               |                       |                        |
| |                                                          |                               |                       |                        |
| +--Field_str (abstract)                                    |                               |                       |                        |
| |  +--Field_longstr                                        |                               |                       |                        |
| |  |  +--Field_string                                      | MYSQL_TYPE_STRING             | MYSQL_TYPE_STRING     |                        |
| |  |  +--Field_varstring                                   | MYSQL_TYPE_VARCHAR            | MYSQL_TYPE_VARCHAR    |                        |
| |  |  +--Field_blob                                        | MYSQL_TYPE_BLOB               |                       |                        |
| |  |     +--Field_geom                                     | MYSQL_TYPE_GEOMETRY           |                       |                        |
| |  |     +--Field_json                                     | MYSQL_TYPE_JSON               |                       |                        |
| |  |        +--Field_typed_array                           | real_type_to_type(m_elt_type) | m_elt_type            | MYSQL_TYPE_TYPED_ARRAY |
| |  |                                                       |                               |                       |                        |
| |  +--Field_null                                           | MYSQL_TYPE_NULL               |                       |                        |
| |  +--Field_enum                                           | MYSQL_TYPE_STRING             | MYSQL_TYPE_ENUM       |                        |
| |     +--Field_set                                         |                               | MYSQL_TYPE_SET        |                        |
| |                                                          |                               |                       |                        |
| +--Field_temporal (abstract)                               |                               |                       |                        |
|    +--Field_time_common (abstract)                         |                               |                       |                        |
|    |  +--Field_time                                        | MYSQL_TYPE_TIME               |                       |                        |
|    |  +--Field_timef                                       | MYSQL_TYPE_TIME               | MYSQL_TYPE_TIME2      | MYSQL_TYPE_TIME2       |
|    |                                                       |                               |                       |                        |
|    +--Field_temporal_with_date (abstract)                  |                               |                       |                        |
|       +--Field_newdate                                     | MYSQL_TYPE_DATE               | MYSQL_TYPE_NEWDATE    |                        |
|       +--Field_temporal_with_date_and_time (abstract)      |                               |                       |                        |
|          +--Field_timestamp                                | MYSQL_TYPE_TIMESTAMP          |                       |                        |
|          +--Field_datetime                                 | MYSQL_TYPE_DATETIME           |                       |                        |
|          +--Field_temporal_with_date_and_timef (abstract)  |                               |                       |                        |
|             +--Field_timestampf                            | MYSQL_TYPE_TIMESTAMP          | MYSQL_TYPE_TIMESTAMP2 | MYSQL_TYPE_TIMESTAMP2  |
|             +--Field_datetimef                             | MYSQL_TYPE_DATETIME           | MYSQL_TYPE_DATETIME2  | MYSQL_TYPE_DATETIME2   |
+------------------------------------------------------------+-------------------------------+-----------------------+------------------------+
```
