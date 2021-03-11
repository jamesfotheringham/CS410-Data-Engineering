def getAggregatedCensus(census):

    subset_census_df = census[['State', 'County', 'TotalPop', 'IncomePerCap', 'Poverty']]

    subset_census_df['PovertyCount'] = subset_census_df['TotalPop'] * (subset_census_df['Poverty'] / 100)

    subset_census_df['Income'] = subset_census_df['IncomePerCap'] * subset_census_df['TotalPop']

    aggregated_census = subset_census_df.groupby(['County', 'State']).agg(Population=('TotalPop','sum'),
                                                                    AggIncomePerCapita=('Income', 'sum'),
                                                                    PovertyCount=('PovertyCount','sum')).reset_index()

    aggregated_census['Poverty'] = (aggregated_census['PovertyCount'] / aggregated_census['Population']) * 100
    aggregated_census['IncomePerCapita'] = aggregated_census['AggIncomePerCapita'] / aggregated_census['Population']

    aggregated_census = aggregated_census[['County', 'State', 'Population', 'Poverty', 'IncomePerCapita']]
    
    return aggregated_census

def getAggregatedCovid(covid, pd):
    covid['county'] = covid['county'] + ' County'
    covid['date'] = pd.to_datetime(covid['date'])

    covid = covid.rename(columns={"county": "County", "state": "State"})

    subset_covid = covid[['date', 'County', 'State', 'cases', 'deaths']]

    aggregated_covid = subset_covid.groupby(['County', 'State']).agg(TotalCases=('cases','sum'),
                                                                        TotalDeaths=('deaths', 'sum')).reset_index()

    december_covid = subset_covid.loc[subset_covid['date'].isin(['2020-12-01','2020-12-31'])]

    aggregated_december = december_covid.groupby(['County', 'State']).agg(Dec2020Cases=('cases','sum'),
                                                                            Dec2020Deaths=('deaths', 'sum')).reset_index()

    covid_data = pd.merge(aggregated_covid, aggregated_december, how='left', on=['County', 'State'])

    return covid_data