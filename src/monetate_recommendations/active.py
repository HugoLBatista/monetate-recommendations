"""
Map recommendation strategies to active experiences.

Action input to recommendation strategy mapping
  long term: explicit action input type for recommendation strategy
  short term: hard coded input field names for current use cases

    type recommendation_set_dict
    type rec_set_slotted_dict
    type rec_set_slotted_json
    type rec_set_predictive_dict
    type site_search_recs_dict
    type site_search_recs_dict_v2
    type templated_recommendation
      type list, name 'rec_set_ids'
        type int
      type list, name 'fallback_rec_set_ids'
        type int

    type product_finder_dict
      type int, name 'recset_id'

    type social_proof_dict
      type int, name 'strategy_id'

Compound recommendation strategy to component recommendation strategy mapping
  long term: explicit parent strategy to child strategy join table
  short term: be conservative and always generate engagement optimized recs

"""
from __future__ import print_function
import contextlib
import datetime

from django.db import connection

RECENT_INPUT_COUNT = """
SELECT count(*)
FROM recs_recommendationset recs
JOIN action_actioninput ri ON recs.id = ri.int_value
JOIN action_actioninput pi ON ri.action_id = pi.action_id  /* parent list or dict */
JOIN action_action aa ON ri.action_id = aa.id
JOIN placement_campaignwhere p ON aa.where_id = p.id
JOIN campaign_campaign c ON p.campaign_id = c.id
JOIN campaign_campaigngroup cg ON c.campaign_group_id = cg.id
WHERE ((ri.name IN ('recset_id', 'strategy_id') AND /* single row join */ pi.lft = 1) OR
       (pi.name IN ('rec_set_ids', 'fallback_rec_set_ids') AND /* list children */ ri.lft > pi.lft AND ri.rgt < pi.rgt))
  AND cg.archived = 0
  AND ((cg.last_modified_time > (now() - INTERVAL 30 DAY)) OR
       (cg.active = 1 AND (cg.end_time IS NULL OR cg.end_time > (now() - INTERVAL 30 DAY))) OR
       (cg.campaign_type = 'email_exp') /* email recommendation experiences are never active */)
  AND recs.id = %s
"""


def is_strategy_active(rs):
    """
    We should generate recommendations only for strategies that are in use,
    or are likely to be in use soon.

    Generate recommendation strategy if:
    - strategy created or updated in the past 30 days
    - strategy referenced by non archived experience modified in the past 30 days
    - strategy referenced by non archived experience active in past 30 days
    - strategy referenced by non archived email recommendation experience

    The current implementation for 'engagement optimized' compound strategies
    uses a JSON string field instead of a proper join table with referential integrity.
    Always generate component strategies until compound strategy implementation fixed.

    :param rs: RecommendationSet
    :return: boolean
    """

    # always generate engagement optimized component strategies
    if rs.is_component_recset:
        return True

    # strategy created or updated in the past 30 days
    if rs.updated >= datetime.datetime.utcnow() - datetime.timedelta(days=30):
        return True

    # strategy referenced by non archived experience modified in the past 30 days
    # strategy referenced by non archived experience active in past 30 days
    with contextlib.closing(connection.cursor()) as cursor:
        cursor.execute(RECENT_INPUT_COUNT, [rs.id])
        count = cursor.fetchone()[0]
        if count > 0:
            return True

    return False
