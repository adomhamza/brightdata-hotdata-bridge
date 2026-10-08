# Results as a table
python examples/query_hotdata.py "SELECT name, current_company.name AS company, array_length(experience) AS jobs FROM public.linkedin"

# JSON or CSV output
python examples/query_hotdata.py --format json "SELECT * FROM public.linkedin LIMIT 5"
python examples/query_hotdata.py --format csv "SELECT name, city FROM public.linkedin" > linkedin.csv

# SQL from a file
python examples/query_hotdata.py --file report.sql