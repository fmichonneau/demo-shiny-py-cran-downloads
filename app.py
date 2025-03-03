import plotnine as plt
import duckdb
from datetime import date, timedelta

from shiny.express import input, render, ui
from shiny import reactive

ui.page_opts(title="R Downloads Explorer")

with ui.sidebar():
    today = date.today()
    three_days_ago = today - timedelta(days=3)
    ui.input_date("date", "Select date", value=three_days_ago, min='2012-10-01', max=three_days_ago)

@reactive.calc
def cran_data():
    if input.date() is None:
        return None
    con = duckdb.connect("memory")
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    year = f"{input.date()}".split("-")[0]
    if not con.execute(f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '{input.date()}')").fetchall()[0][0]:
        con.execute(f"CREATE TABLE '{input.date()}' AS SELECT * FROM read_csv('http://cran-logs.rstudio.com/{year}/{input.date()}-r.csv.gz')")
    # results = con.sql(f"SELECT * FROM '{input.date()}'").df()
    return con

@reactive.calc
def cran_data_by_country():
    if input.date() is None:
        return None
    con = cran_data()
    res = con.sql(f"WITH unique_ips AS (SELECT DISTINCT ip_id, country FROM '{input.date()}') SELECT country, COUNT(country) AS n_downloads_by_country FROM unique_ips GROUP BY country ORDER BY COUNT(country) DESC LIMIT 20").df()
    return res


@render.plot
def hist():
    if input.date() is None:
        return None
    data = cran_data_by_country()
    countries = data["country"].value_counts().index.tolist()[::-1]
    res = (
        plt.ggplot(data, plt.aes(x="country", y = "n_downloads_by_country"))
        + plt.labs(
            x="Country", y="Number of Downloads",
            title = f"Top 20 Countries by Downloads on {input.date()}",
            subtitle = f"Filtered on unique IP addresses")
        + plt.geom_col()
        + plt.scale_x_discrete(limits = countries)
        + plt.scale_y_log10()
        + plt.coord_flip()
        + plt.theme_light()
    )
    return res
