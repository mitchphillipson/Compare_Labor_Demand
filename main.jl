using Pkg
Pkg.activate(".")
Pkg.instantiate()

using WiNDCNational
using DataFrames
using CSV

summary_raw = build_us_table(:summary)
summary, _ = calibrate(summary_raw)

data_dir = "blog/009_disaggregate_state/data_compare/data"




state_fips = CSV.read(
    joinpath(data_dir,"state_fips.csv"), 
    DataFrame,
    types = Dict(
        :fips => String,
        :state => String,
        ),
    select = [:fips, :state]
    )



industry_codes = CSV.read(
    joinpath(data_dir,"industry_codes.csv"), 
    DataFrame,
    types = Dict(:naics => Symbol),
    drop = [:Description]
    ) |>
    dropmissing



function load_sagdp(
    path::String; 
    data_dir = data_dir,
    industry_codes::DataFrame = industry_codes,
    state_fips::DataFrame = state_fips,
    )

    df = CSV.read(
        joinpath(data_dir,path), 
        DataFrame,
        footerskip = 4,
        missingstring = ["(NA)", "(D)", "(NM)","(L)", "(T)"],
        drop = [:GeoName, :Region, :TableName, :IndustryClassification],
        types = Dict(:GeoFIPS => String)
        ) |>
        x -> stack(x, Not(:GeoFIPS, :LineCode, :Unit, :Description), variable_name=:year, value_name=:value) |>
        dropmissing |>
        x -> transform(x, 
            :year => ByRow(y -> parse(Int, y)) => :year,
            :value => (y -> y./1_000_000) => :value, #Convert to billions of dollars
        ) |>
        x -> innerjoin(x, state_fips, on = :GeoFIPS => :fips) |>
        x -> innerjoin(x, industry_codes, on = :LineCode) |>
        x -> select(x, Not(:GeoFIPS, :LineCode, :Unit, :Description)) |>
        x -> subset(x, :value => ByRow(!iszero))

    return df
end

### Labor
labor_path = "SAGDP4__ALL_AREAS_1997_2023.csv"
labor = load_sagdp(labor_path)

aggregated_labor = labor |>
    x -> groupby(x, [:year, :naics]) |>
    x -> combine(x, :value => sum => :labor)


# Compare Summary data to SAGDP data
df = outerjoin(
    table(summary, :Labor_Demand),
    aggregated_labor,
    on = [:year, :col => :naics],
) |>
x -> transform(x, [:value,:labor] => ByRow((+)) => :diff)

df |>
    x -> dropmissing(x, :diff) |>
    x -> subset(x, :diff => ByRow(y-> abs(y) < 1)) |>
    x -> unique(x, :col)



table(summary, :Labor_Demand) |>
    x -> groupby(x, :year) |>
    x -> combine(x, :value => sum => :total)

labor |>
    x -> groupby(x, :year) |>
    x -> combine(x, :value => sum => :total)