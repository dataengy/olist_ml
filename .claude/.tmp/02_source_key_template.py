"""defs.yaml: ключ источника = <source_name>/<table>, иначе node.name (дубль seed/source)."""
p = "src/olist_ml/defs/dbt/defs.yaml"; s = open(p).read()
s = s.replace('    key: "{{ node.name }}"',
  "    key: \"{{ node.source_name ~ '/' ~ node.name if node.resource_type == 'source' else node.name }}\"")
open(p, "w").write(s)
