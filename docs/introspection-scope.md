# Introspection Scope

The `introspection-scope` configuration defines which catalogs and schemas are introspected by a database plugin.  
It provides a declarative way to restrict introspection to a specific subset of the database, avoiding unnecessary or expensive scans.

The scope operates at the (catalog, schema) level.

---

## Scope of application

- A **catalog + schema** pair that is *in scope* is introspected.
- A **catalog + schema** pair that is *out of scope* is skipped.
- Filtering is applied **before** any heavy introspection work.

For database engines that do not support catalogs, the engine is treated as having a single implicit catalog. In such cases, rules omit the `catalog` field.

---

## YAML configuration

```yaml
introspection-scope:
  include:
    - catalog: <glob-pattern>     # optional
      schemas: [<glob>, <glob>]   # optional (string also allowed)
  exclude:
    - catalog: <glob-pattern>     # optional
      schemas: [<glob>, <glob>]   # optional (string also allowed)
      except_schemas: [<glob>]    # optional (string also allowed)
```

### Rules

Each entry under `include` or `exclude` is a **rule**.

A rule may contain:

- `catalog`  
  A glob pattern matching catalog names.  
  If omitted, the rule matches **any catalog**.

- `schemas`  
  One or more glob patterns matching schema names.  
  If omitted, the rule matches **any schema**.

- `except_schemas` (exclude rules only)  
  One or more glob patterns defining schema exceptions.  
  If a schema matches `except_schemas`, it is **not excluded by that rule**, even if the rule otherwise matches.

Each rule **must specify at least one** of `catalog` or `schemas`.

---

## Glob pattern matching

All matching uses **glob patterns** and is **case-insensitive**.

### Supported glob syntax

| Pattern | Meaning | Example |
|------|--------|--------|
| `*` | Matches any number of characters (including zero) | `sales_*` matches `sales_2024` |
| `?` | Matches exactly one character | `dev?` matches `dev1`, `devA` |
| `[seq]` | Matches any single character in `seq` | `dev[12]` matches `dev1`, `dev2` |
| `[!seq]` | Matches any single character **not** in `seq` | `dev[!0-9]` matches `devA` |

### Examples

- `analytics`  
  Matches only `analytics`

- `sales_*`  
  Matches `sales_2024`, `sales_tmp`

- `*tmp*`  
  Matches `tmp`, `my_tmp_schema`, `schema_tmp_v2`

- `dev[!0-9]`  
  Matches `devA`, but not `dev1`

## Semantics and precedence

Scope evaluation follows these rules:

### 1. Initial scope selection

- If `include` is **absent or empty**, the initial scope consists of **all discovered catalogs and schemas**.
- If `include` is **present and non-empty**, the initial scope consists only of catalog+schema pairs that match **at least one include rule**.

---

### 2. Exclusion

After the initial scope is determined:

- Any catalog+schema pair that matches **any exclude rule** is removed.
- Exclusion always takes precedence over inclusion (**exclude wins**).
- `except_schemas` applies only to the exclude rule in which it is defined and prevents that rule from excluding matching schemas.

Exclude rules are combined using **OR** semantics.

---

## Examples

### Exclude system schemas everywhere

```yaml
introspection-scope:
  exclude:
    - schemas: [information_schema, sys]
```

Result:
- All catalogs and schemas are introspected except `information_schema` and `sys`.

---

### Restrict introspection to specific schemas in one catalog

```yaml
introspection-scope:
  include:
    - catalog: hive
      schemas: [analytics, sales_*]
```

Result:
- Only `analytics` and `sales_*` schemas in the `hive` catalog are introspected.

---

### Introspect one schema in a catalog, everything elsewhere

```yaml
introspection-scope:
  exclude:
    - catalog: A
      schemas: ["*"]
      except_schemas: [B]
```

Result:
- In catalog `A`, only schema `B` is introspected.
- All schemas in all other catalogs remain in scope.
