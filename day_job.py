import api_v0 as api
import pandas as pd
import json
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pathlib import Path


def main():
    """ get yesterday's DMPs (retroactive data jobs)
    """
    yesterday = str((datetime.today() - timedelta(days=1)).date())
    plans = api.retrieve_plans(yesterday)
    pages = list(plans)
    # create folder if it doesn't exist
    Path('dmps').mkdir(parents=True, exist_ok=True)
    # keep json as backup
    with open(f'dmps/{yesterday}.json', 'w') as f:
        f.write(json.dumps(pages))
    # transform to table, store as csv
    plans_df = transform(pages)
    plans_df.to_csv(f'dmps/{yesterday}.csv', index=False)


def transform(pages):
    """ transform from json to table format
    """
    plans = pd.DataFrame()
    for page in pages:
        # we have three levels: metadata, sections, questions-answers
        # split into individual tables, don't keep the columns with nested data
        metadata = pd.json_normalize(page).drop(columns='plan_content')
        sections = pd.json_normalize(page, ['plan_content', ['sections']],
                                     meta=['id'])  # we keep the DMP id for the join
        qa = pd.DataFrame()
        for index, row in sections.iterrows():
            aux = pd.json_normalize(row['questions'])
            aux[['id', 'section']] = row[['id', 'number']]  # keep for join
            qa = qa.append(aux)
        # remove markup
        qa[['text', 'answer.text']] = qa[['text', 'answer.text']].applymap(
            lambda x: BeautifulSoup(x, "lxml").text if not pd.isna(x) else '')
        # don't keep nested questions in sections table
        sections = sections.drop(columns='questions')
        # merge tables
        merged = metadata.merge(sections.merge(qa,
                                               how='left',
                                               left_on=['number', 'id'], right_on=['section', 'id'],
                                               suffixes=('_section', '_question')),
                                how='left', on='id',
                                suffixes=('_dmp', '_section'))
        # remove test plans
        merged = merged[~merged['test_plan']]
        plans = plans.append(merged)

    return plans


if __name__ == "__main__":
    main()
