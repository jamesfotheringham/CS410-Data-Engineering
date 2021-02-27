import pandas as pd

pd.set_option('display.width', 400)
pd.set_option('display.max_rows', 50)

# Read in census data and aggregate
census_df = pd.read_csv("acs2017_census_tract_data.csv")

subset_census_df = census_df[['State', 'County', 'TotalPop', 'IncomePerCap', 'Poverty']]

subset_census_df['PovertyCount'] = subset_census_df['TotalPop'] * (subset_census_df['Poverty'] / 100)

subset_census_df['Income'] = subset_census_df['IncomePerCap'] * subset_census_df['TotalPop']

aggregated_census = subset_census_df.groupby(['County', 'State']).agg(Population=('TotalPop','sum'),
                                                                 AggIncomePerCapita=('Income', 'sum'),
                                                                 PovertyCount=('PovertyCount','sum')).reset_index()

aggregated_census['Poverty'] = (aggregated_census['PovertyCount'] / aggregated_census['Population']) * 100
aggregated_census['IncomePerCapita'] = aggregated_census['AggIncomePerCapita'] / aggregated_census['Population']

aggregated_census = aggregated_census[['County', 'State', 'Population', 'Poverty', 'IncomePerCapita']]

loudoun_county = aggregated_census.loc[(aggregated_census['State'] == 'Virginia') & (aggregated_census['County'] == 'Loudoun County')]
washington_county = aggregated_census.loc[(aggregated_census['State'] == 'Oregon') & (aggregated_census['County'] == 'Washington County')]
harlan_county = aggregated_census.loc[(aggregated_census['State'] == 'Kentucky') & (aggregated_census['County'] == 'Harlan County')]
malheur_county = aggregated_census.loc[(aggregated_census['State'] == 'Oregon') & (aggregated_census['County'] == 'Malheur County')]

acs_counties = pd.concat([loudoun_county, washington_county, harlan_county, malheur_county])
print(acs_counties)

# Read in covid data and aggregate
covid_df = pd.read_csv("COVID_county_data.csv")
covid_df['county'] = covid_df['county'] + ' County'
covid_df['date'] = pd.to_datetime(covid_df['date'])

covid_df = covid_df.rename(columns={"county": "County", "state": "State"})

subset_covid_df = covid_df[['date', 'County', 'State', 'cases', 'deaths']]

aggregated_covid = subset_covid_df.groupby(['County', 'State']).agg(TotalCases=('cases','sum'),
                                                                    TotalDeaths=('deaths', 'sum')).reset_index()

december_covid_df = subset_covid_df.loc[subset_covid_df['date'].isin(['2020-12-01','2020-12-31'])]

aggregated_december = december_covid_df.groupby(['County', 'State']).agg(Dec2020Cases=('cases','sum'),
                                                                         Dec2020Deaths=('deaths', 'sum')).reset_index()

covid_data = pd.merge(aggregated_covid, aggregated_december, how='left', on=['County', 'State'])

loudoun_county_covid = covid_data.loc[(covid_data['State'] == 'Virginia') & (covid_data['County'] == 'Loudoun County')]
washington_county_covid = covid_data.loc[(covid_data['State'] == 'Oregon') & (covid_data['County'] == 'Washington County')]
harlan_county_covid = covid_data.loc[(covid_data['State'] == 'Kentucky') & (covid_data['County'] == 'Harlan County')]
malheur_county_covid = covid_data.loc[(covid_data['State'] == 'Oregon') & (covid_data['County'] == 'Malheur County')]

covid_counties = pd.concat([loudoun_county_covid, washington_county_covid, harlan_county_covid, malheur_county_covid])

print(covid_counties)

# Integrate covid and acs data sets.
integrated_df = pd.merge(aggregated_census, covid_data, how='left', on=['County', 'State'])
oregon_integrated_df = integrated_df.loc[(integrated_df['State'] == 'Oregon')]
print(oregon_integrated_df)

# Perform data analysis

oregon_integrated_df['normalized_total_cases'] = (oregon_integrated_df['TotalCases'] * 100000) / oregon_integrated_df['Population']
oregon_integrated_df['normalized_december_cases'] = (oregon_integrated_df['Dec2020Cases'] * 100000) / oregon_integrated_df['Population']
oregon_integrated_df['normalized_total_deaths'] = (oregon_integrated_df['TotalDeaths'] * 100000) / oregon_integrated_df['Population']
oregon_integrated_df['normalized_december_deaths'] = (oregon_integrated_df['Dec2020Deaths'] * 100000) / oregon_integrated_df['Population']


oregon_total_cases_poverty_correlation = oregon_integrated_df['normalized_total_cases'].corr(oregon_integrated_df['Poverty'])
oregon_total_deaths_poverty_correlation = oregon_integrated_df['normalized_total_deaths'].corr(oregon_integrated_df['Poverty'])
oregon_total_cases_income_correlation = oregon_integrated_df['normalized_total_cases'].corr(oregon_integrated_df['IncomePerCapita'])
oregon_total_deaths_income_correlation = oregon_integrated_df['normalized_total_deaths'].corr(oregon_integrated_df['IncomePerCapita'])
oregon_dec_cases_poverty_correlation = oregon_integrated_df['normalized_december_cases'].corr(oregon_integrated_df['Poverty'])
oregon_dec_deaths_poverty_correlation = oregon_integrated_df['normalized_december_deaths'].corr(oregon_integrated_df['Poverty'])
oregon_dec_cases_income_correlation = oregon_integrated_df['normalized_december_cases'].corr(oregon_integrated_df['IncomePerCapita'])
oregon_dec_deaths_income_correlation = oregon_integrated_df['normalized_december_deaths'].corr(oregon_integrated_df['IncomePerCapita'])