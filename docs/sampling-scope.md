# Sampling Scope

The `sampling` configuration defines which tables are allowed to have **row samples** collected during database introspection.

This feature is meant for customers who want metadata introspection (catalogs/schemas/tables/columns) but want to avoid reading actual row values from objects they consider sensitive.

The scope operates at the **(catalog, schema, table)** level.

---

## Scope of application

- A **catalog + schema + table** that is *in sampling scope* may have sample rows collected.
- A **catalog + schema + table** that is *out of sampling scope* will **not** have sample rows collected.
- Sampling scope is applied **only to the sampling step** (i.e., “collect samples from each table”).
- Sampling scope does **not** change what is introspected for metadata. Use `introspection-scope` to control metadata introspection.

For database engines that do not support catalogs, the engine is treated as having a single implicit catalog. In such cases, rules omit the `catalog` field.

---

## YAML configuration

```yaml
sampling:
  enabled: true                     # optional (default: true)
  scope:                            # optional
    include:
      - catalog: <glob-pattern>     # optional
        schemas: [<glob>, <glob>]   # optional (string also allowed)
        tables:  [<glob>, <glob>]   # optional (string also allowed)
    exclude:
      - catalog: <glob-pattern>     # optional
        schemas: [<glob>, <glob>]   # optional (string also allowed)
        tables:  [<glob>, <glob>]   # optional (string also allowed)
        except_schemas: [<glob>]    # optional (string also allowed)
        except_tables:  [<glob>]    # optional (string also allowed)
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

- `tables`  
  One or more glob patterns matching table names.  
  If omitted, the rule matches **any table**.

- `except_schemas` / `except_tables` (exclude rules only)  
  One or more glob patterns defining exceptions.  
  If a target matches `except_schemas` / `except_tables`, it is **not excluded by that rule**, even if the rule otherwise matches.

Each rule **must specify at least one** of `catalog`, `schemas`, or `tables`.

---

## Glob pattern matching

All matching uses **glob patterns** and is **case-insensitive**.

### Supported glob syntax

| Pattern | Meaning | Example |
|------|--------|--------|
| `*` | Matches any number of characters (including zero) | `order_*` matches `order_items` |
| `?` | Matches exactly one character | `dev?` matches `dev1`, `devA` |
| `[seq]` | Matches any single character in `seq` | `dev[12]` matches `dev1`, `dev2` |
| `[!seq]` | Matches any single character **not** in `seq` | `dev[!0-9]` matches `devA` |

---

## Semantics and precedence

Sampling scope evaluation follows these rules:

### 1. Master switch

- If `sampling.enabled` is `false`, **no tables are sampled**, regardless of `scope`.

### 2. Initial scope selection

- If `scope.include` is **absent or empty**, the initial sampling scope consists of **all discovered tables** (subject to `introspection-scope` and any ignored schemas).
- If `scope.include` is **present and non-empty**, the initial sampling scope consists only of tables that match **at least one include rule**.

### 3. Exclusion

After the initial scope is determined:

- Any table that matches **any exclude rule** is removed from the sampling scope.
- Exclusion always takes precedence over inclusion (**exclude wins**).
- `except_schemas` / `except_tables` apply only to the exclude rule in which they are defined and prevent that rule from excluding matching targets.

Exclude rules are combined using **OR** semantics.

---

## Relationship to introspection-scope

Sampling rules are evaluated **only for objects that are introspected**.

- If a schema is excluded by `introspection-scope`, its tables are not introspected and therefore cannot be sampled.
- `sampling` only controls whether sample rows are collected for tables that are otherwise discovered and introspected.

---

## Examples

### Disable sampling entirely

```yaml
sampling:
  enabled: false
```

Result:
- Metadata introspection still runs (subject to `introspection-scope`).
- No sample rows are collected from any table.

---

### Exclude a schema everywhere

```yaml
sampling:
  scope:
    exclude:
      - schemas: [hr]
```

Result:
- Tables in `hr` are not sampled.
- Tables in other schemas remain eligible for sampling.

---

### Allowlist only a subset of tables

```yaml
sampling:
  scope:
    include:
      - schemas: [analytics]
        tables: ["revenue_*"]
```

Result:
- Only `analytics.revenue_*` tables are sampled.
- Everything else is not sampled.

---

### Exclude sensitive tables but sample everything else

```yaml
sampling:
  scope:
    exclude:
      - schemas: [shop]
        tables: [customers]
      - schemas: [hr]
        tables: [employees, active_employees]
```

Result:
- `shop.customers`, `hr.employees`, and `hr.active_employees` are not sampled.
- All other tables remain eligible for sampling.

---

### Exclude an entire schema, but allow a few safe tables

```yaml
sampling:
  scope:
    exclude:
      - schemas: [hr]
        except_tables: [departments, orders_by_employee]
```

Result:
- In `hr`, only `departments` and `orders_by_employee` are sampled.
- All other `hr` tables are not sampled.
