from __future__ import print_function, division
weekend_update_categories = {'Weekend Update', 'Saturday Night News', 'SNL Newsbreak'}
live_sketch_categories = {'Sketch', 'Musical Sketch', 'Show', 'Game Show', 'Award Show'}
recorded_sketch_categories = {'Film', 'Commercial'}
# (See note in items.py re Miscellaneous category)
misc_performer_categories = {'Cold Opening', 'Monologue', 'Miscellaneous'}

# These are the categories of titles that count when computing airtime statistics. 
# Main omissions are Goodnights and Musical Performance. Also some rarer categories
# like Guest Performance, In Memoriam, Talent Entrance, etc.
performer_title_categories = set.union(
  misc_performer_categories, weekend_update_categories, live_sketch_categories, recorded_sketch_categories
)
def add_airtime_columns(titles, episodes, apps):
  """Add some derived columns to titles/episodes that are useful when calculating relative 'airtime'
  of cast members and guests.
  """
  # If there are n eligible titles in an episode, each has an episode_share of 1/n
  titles['episode_share'] = 0.0
  titles['n_performers'] = 0
  # The same as above, but each title is further normalized by number of performers present.
  titles['cast_episode_share'] = 0.0
  for episode in episodes.itertuples():
    ep_titles_ix = ((titles['epid']==episode.epid)
      & (titles['category'].isin(performer_title_categories)))
    n_titles = ep_titles_ix.sum()
    if n_titles == 0:
      print('Warning: Found 0 titles for epid {}. Skipping.'.format(episode.epid))
      continue
      
    titles.loc[ep_titles_ix, 'episode_share'] = 1/n_titles
    perfs = []
    for title in titles[ep_titles_ix].itertuples():
      performer_aids = apps[apps['tid']==title.tid]['aid'].unique()
      perfs.append(len(performer_aids))
    titles.loc[ep_titles_ix, 'n_performers'] = perfs
    titles.loc[ep_titles_ix, 'cast_episode_share'] = (
      titles.loc[ep_titles_ix, 'episode_share'] 
      / 
      titles.loc[ep_titles_ix, 'n_performers']
    )
