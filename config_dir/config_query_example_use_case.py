# -*- coding: utf-8 -*-
"""
Created on Fri Nov 12 18:11:58 2021

@author: aless
"""

def get_target_metric_in_perim_query():
    """
    In this functin you need to write, in the input_query string, the query that define the metric you are interested
    in, for the target value of the dimension on which the campaing in performed. In other term it define the metric
    on which you want to evaluate the effetc of the campaing, for the audience specified by the target value of the 
    campaing dimension, wich is then affetced by the campaign you want to evaluate.
    
    The campaing dimension is the feature that define which is the audience of the campaing and has to be be chosen
    in the way that the target group of population has a specific value for it, while the rest of the
    population can be identified by different values of it, which are called control values (e.g country).
    
    The perimeter is defined as the eventual constriction, on the target population defined by the target value of the
    campaing dimension, that contribute to define the target of the campaign(e.g a particular lesson subject,a particular
    paltform language, a particular channel,...). This last one is embedded in the query trough hardcoded filters
    and is optional to include.
    
    The query accepts three parameters, named {{ start }}, {{ end }} and {{ target }} which need to be expressed
    within double graph bracket.
    The first one states the starting date of the observation period while the second is the ending date.
    The last parameter indicate the value of the campaing dimension (which has to be expressed in the query) equal to the
    target.
    
    The final form of the data has to be a table of three columns named reference_date, campaing_dimension and metric.
    
    """
    input_query = """select A.date as reference_date, A.country_code as campaign_dimension, sum(A.npc_first_touch) as metric
                        from(
                        select *
                        from default.ci_anl_table
                        where date between '{start}' and '{end}' 
                        and country_code = '{target}'
                        )A
                        group by A.date, A.country_code
                        order by A.country_code, A.date
                    """
    return input_query


def get_target_metric_out_perim_query():
    """
    In this function you need to write, in the input_query string, the query that define your metric for the population
    that has the target value of the campaign_dimension (e.g it is in the target country) but do not posses the eventual
    additional constriction that specify the perimeter of your campaing and so it is not affetected by the campaign you want
    to evaluate. The absence of this condition is hardcoded in some filters of the query.
    
    The usage of this query is optional, if you do not want to use it, leave the input_query string empty. In this case
    remeber to set in the Notebook the parameter out_perim_usage as False.
    
    The query accepts three parameters, named {{ start }}, {{ end }} and {{ target }} which need to be expressed
    within double graph bracket.
    The first one states the starting date of the observation period while the second is the ending date.
    The last parameter indicate the value of the campaing dimension (which has to be expressed in the query) equal to the
    target.
    
    The final form of the data has to be a table of three columns named reference_date, campaing_dimension and metric
    
    """
    input_query =  """
                    """
    return input_query



def get_target_metric_control_query():
    """
    In this function you need to write, in the input_query string, the query that define your metric for the part of
    population that is not affeted by the campaing you want to evaluate. In other terms this query define your metric
    for different groups of population, identified by the campaign dimension in a specific control group
    of values, different from the target.
    
    This query must be defined and that is why the camapign dimension has to be chosen so that it is possible to identify
    for it, a target value and a control group of values.
    
    For the definition of this query, if the value of the campaign dimension in the contorl group, strictly identified groups
    of population not affetceted by the campaing, it is suggetsed to not include eventual additional constriction that
    define your campaing. If otherwise havinge the campaing dimension in the control group of value do not ensure that 
    the population highlighted by this query is not affected by the campaing, a more strictly contriction has to be harcoded
    in the query. The basic idea is that this query identify the chosen metric, in group of populatioin not affected by the
    campaign you want to evaluate.
    
    The query accepts three parameters, named {{ start }}, {{ end }} and {{ control | inclause }} which need to be expressed
    within double graph bracket. Note that the third paramether (control) MUST be followed, inside the parenthesis,
    by the string | inclause.
    The first one states the starting date of the observation period while the second is the ending date
    The last parameter indicate the value of the campaing dimension (which has to be expressed in the query) inside the
    control group
    
    The final form of the data has to be a table of three columns named reference_date, campaing_dimension and metric
    
    """
    
    input_query = """select A.date as reference_date, A.country_code as campaign_dimension, sum(A.npc_first_touch) as metric
                        from(
                        select *
                        from default.ci_anl_table
                        where date between '{start}' and '{end}' 
                        and country_code in {control}
                        )A
                        group by A.date, A.country_code
                        order by A.country_code, A.date
                    """
    return input_query   
    

def get_visitor_control_query():
    """
    In this function you need to write, in the input_query string, the query that define the visit to the preply
    website for the part of population that is not affeted by the campaing you want to evaluate. In other terms this query
    define the number of visit to the preply website for different groups of population, identified by the campaign dimension
    in a specific control group of values, different from the target.
    
    This query must be defined and that is why the camapign dimension has to be chosen so that it is possible to identify
    for it, a target value and a control group of values
    
    For the definition of this query, if the value of the campaign dimension in the contorl group, strictly identified groups
    of population not affetceted by the campaing, it is suggetsed to not include eventual additional constriction that
    define your campaing. If otherwise havinge the campaing dimension in the control group of value do not ensure that 
    the population highlighted by this query is not affected by the campaing, a more strictly contriction has to be harcoded
    in the query. The basic idea is that this query identify the preply visit, in group of populatioin not affected by the
    campaign you want to evaluate.
    
    It is also suggested to define this query in order to take in consideration visit with directly related 
    to the metric on which the campaign has to be evaluated, and this condition has to be hardcoded in the query (e.g. is the metric is npc
    the visit shuould be of traffic_group different than CONTENT)
    
    The query accepts three parameters, named {{ start }}, {{ end }} and {{ control | inclause }} which need to be expressed
    within double graph bracket. Note that the third paramether (control) MUST be followed, inside the parenthesis,
    by the string | inclause.
    The first one states the starting date of the observation period while the second is the ending date
    The last parameter indicate the value of the campaing dimension (which has to be expressed in the query) inside the
    control group
    
    The final form of the data has to be a table of three columns named reference_date, campaing_dimension and visitor
    
    """
    input_query = """select date as reference_date, country_code as campaign_dimension, sum(new_visitors) as visitor
                        from default.ci_anl_table
                        where date between '{start}' and '{end}'
                        and traffic_group != 'CONTENT'
                        and country_code in {control}
                        group by date, country_code
                        order by country_code, date
                    """
    
    return input_query



def get_click_control_query():
    """
    In this function you need to write, in the input_query string, the query that define the click to the preply
    website for the part of population that is not affeted by the campaing you want to evaluate. In other terms this query
    define the number of click to the preply website for different groups of population, identified by the campaign dimension
    in a specific control group of values, different from the target.
    
    This query must be defined and that is why the camapign dimension has to be chosen so that it is possible to identify
    for it, a target value and a control group of values
    
    For the definition of this query, if the value of the campaign dimension in the contorl group, strictly identified groups
    of population not affetceted by the campaing, it is suggetsed to not include eventual additional constriction that
    define your campaing. If otherwise havinge the campaing dimension in the control group of value do not ensure that 
    the population highlighted by this query is not affected by the campaing, a more strictly contriction has to be harcoded
    in the query. The basic idea is that this query identify the clicks, in group of populatioin not affected by the
    campaign you want to evaluate.
    
    It is also suggested to define this query in order to take in consideration click with directly related 
    to the metric on which the campaign has to be evaluated, and this condition has to be hardcoded in the query (e.g. is the metric is npc
    the visit shuould be of traffic_group different than CONTENT)
    
    If the target metric of the campaign is the click, leave the query string empty and set the parameter click_usage as False
    
    The query accepts three parameters, named {{ start }}, {{ end }} and {{ control | inclause }} which need to be expressed
    within double graph bracket. Note that the third paramether (control) MUST be followed, inside the parenthesis,
    by the string | inclause.
    The first one states the starting date of the observation period while the second is the ending date
    The last parameter indicate the value of the campaing dimension (which has to be expressed in the query) inside the
    control group
    
    The final form of the data has to be a table of three columns named reference_date, campaing_dimension and click
    
    """
    
    input_query = """select date as reference_date, country_code as campaign_dimension, sum(clicks) as click
                        from default.ci_anl_table
                        where date between '{start}' and '{end}'
                        and traffic_group != 'CONTENT'
                        and country_code in {control}
                        group by date, country_code
                        order by country_code, date
                """
                
    return input_query
    

def get_impression_control_query():
    """
    In this function you need to write, in the input_query string, the query that define the impression to the preply
    website for the part of population that is not affeted by the campaing you want to evaluate. In other terms this query
    define the number of impressionto the preply website for different groups of population, identified by the campaign dimension
    in a specific control group of values, different from the target.
    
    This query must be defined and that is why the camapign dimension has to be chosen so that it is possible to identify
    for it, a target value and a control group of values
    
    For the definition of this query, if the value of the campaign dimension in the contorl group, strictly identified groups
    of population not affetceted by the campaing, it is suggetsed to not include eventual additional constriction that
    define your campaing. If otherwise havinge the campaing dimension in the control group of value do not ensure that 
    the population highlighted by this query is not affected by the campaing, a more strictly contriction has to be harcoded
    in the query. The basic idea is that this query identify the impression, in group of populatioin not affected by the
    campaign you want to evaluate.
    
    It is also suggested to define this query in order to take in consideration impression with directly related 
    to the metric on which the campaign has to be evaluated, and this condition has to be hardcoded in the query (e.g. is the metric is npc
    the visit shuould be of traffic_group different than CONTENT)
    
    If the target metric of the campaign is the impression, leave the query string empty and set the parameter impression_usage as False
    
    The query accepts three parameters, named {{ start }}, {{ end }} and {{ control | inclause }} which need to be expressed
    within double graph bracket. Note that the third paramether (control) MUST be followed, inside the parenthesis,
    by the string | inclause.
    The first one states the starting date of the observation period while the second is the ending date
    The last parameter indicate the value of the campaing dimension (which has to be expressed in the query) inside the
    control group
    
    The final form of the data has to be a table of three columns named reference_date, campaing_dimension and impression
    
    """
    
    input_query = """select date as reference_date, country_code as campaign_dimension, sum(impressions) as impression
                        from default.ci_anl_table
                        where date between '{start}' and '{end}'
                        and traffic_group != 'CONTENT'
                        and country_code in {control}
                        group by date, country_code
                        order by country_code, date
                """
                
    return input_query
    

def get_npc_control_query():
    """
    In this function you need to write, in the input_query string, the query that define the new paying customer
    for the part of population that is not affeted by the campaing you want to evaluate. In other terms this query
    define the number of NPC for different groups of population, identified by the campaign dimension
    in a specific control group of values, different from the target.
    
    This query must be defined and that is why the camapign dimension has to be chosen so that it is possible to identify
    for it, a target value and a control group of values
    
    For the definition of this query, if the value of the campaign dimension in the contorl group, strictly identified groups
    of population not affetceted by the campaing, it is suggetsed to not include eventual additional constriction that
    define your campaing. If otherwise havinge the campaing dimension in the control group of value do not ensure that 
    the population highlighted by this query is not affected by the campaing, a more strictly contriction has to be harcoded
    in the query. The basic idea is that this query identify the new_paying_customer, in group of populatioin not affected by the
    campaign you want to evaluate.
    
    
    If the target metric of the campaign is the new_paying_customer, leave the query string empty and set the parameter npc_usage as False
    
    The query accepts three parameters, named {{ start }}, {{ end }} and {{ control | inclause }} which need to be expressed
    within double graph bracket. Note that the third paramether (control) MUST be followed, inside the parenthesis,
    by the string | inclause.
    The first one states the starting date of the observation period while the second is the ending date
    The last parameter indicate the value of the campaing dimension (which has to be expressed in the query) inside the
    control group
    
    The final form of the data has to be a table of three columns named reference_date, campaing_dimension and npc
    
    """
    
    input_query = """
                """
                
    return input_query

    
    