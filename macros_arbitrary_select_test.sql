.mode markdown
.null NULL

CREATE OR REPLACE TABLE Cities (
    Country VARCHAR, Name VARCHAR, Year INTEGER, Population INTEGER
);
INSERT INTO Cities VALUES
    ('NL', 'Amsterdam', 2000, 1005),
    ('NL', 'Amsterdam', 2010, 1065),
    ('NL', 'Amsterdam', 2020, 1158),
    ('US', 'Seattle', 2000, 564),
    ('US', 'Seattle', 2010, 608),
    ('US', 'Seattle', 2020, 738),
    ('US', 'New York City', 2000, 8015),
    ('US', 'New York City', 2010, 8175),
    ('US', 'New York City', 2020, 8772);
 
FROM Cities;

SET VARIABLE column_names = (SELECT build_column_names(['cities'], ['year'], []));


FROM pivot_table_show_sql(['SELECT Country, Name, Year, Population FROM Cities'],
                 ['min(Population)', 'max(Population)'],
                 ['Country', 'Name'],
                 ['Year'],
                 getvariable('column_names'),
                 [],
                 subtotals:=1,
                 grand_totals:=1,
                 values_axis:='rows'
                 );

FROM pivot_table(['FROM Cities SELECT Country, Name, Year, Population'],
                 ['min(Population)', 'max(Population)'],
                 ['Country', 'Name'],
                 ['Year'],
                 getvariable('column_names'),
                 [],
                 subtotals:=1,
                 grand_totals:=1,
                 values_axis:='rows'
                 );

FROM pivot_table(['pivot_table_functions.main.cities'],
                 ['min(Population)', 'max(Population)'],
                 ['Country', 'Name'],
                 ['Year'],
                 getvariable('column_names'),
                 [],
                 subtotals:=1,
                 grand_totals:=1,
                 values_axis:='rows'
                 );

-- Test multiple values with values_axis columns
FROM pivot_table(['FROM Cities SELECT Country, Name, Year, Population'],
                 ['min(Population)', 'max(Population)'],
                 ['Country', 'Name'],
                 ['Year'],
                 getvariable('column_names'),
                 [],
                 subtotals:=1,
                 grand_totals:=1,
                 values_axis:='columns'
                 );

-- Test single values column_name
FROM pivot_table(['FROM Cities SELECT Country, Name, Year, Population'],
                 ['min(Population)'],
                 ['Country', 'Name'],
                 ['Year'],
                 getvariable('column_names'),
                 [],
                 subtotals:=1,
                 grand_totals:=1,
                 values_axis:='columns'
                 );

-- Test multiple values with values_axis columns but no columns pivoted outward
FROM pivot_table(['FROM Cities SELECT Country, Name, Year, Population'],
                 ['min(Population)', 'max(Population)'],
                 ['Country', 'Name'],
                 [],
                 [],
                 [],
                 subtotals:=1,
                 grand_totals:=1,
                 values_axis:='columns'
                 );

SET VARIABLE column_names = (SELECT build_column_names(['cities'], ['Country', 'Name'], []));

FROM pivot_table(['pivot_table_functions.main.cities'],
                 ['min(Population)', 'max(Population)'],
                 ['Year'],
                 ['Country', 'Name'],
                 getvariable('column_names'),
                 [],
                 subtotals:=1,
                 grand_totals:=1,
                 values_axis:='rows'
                 );

FROM pivot_table(['pivot_table_functions.main.cities'],
                 ['min(Population)', 'max(Population)'],
                 ['Year'],
                 ['Country', 'Name'],
                 getvariable('column_names'),
                 [],
                 subtotals:=1,
                 grand_totals:=1,
                 values_axis:='columns'
                 );