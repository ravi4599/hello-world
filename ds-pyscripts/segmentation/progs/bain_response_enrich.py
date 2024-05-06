def bain_response_enrich(res):
    """
    Requires: numpy, pandas
    Purpose: Enrich a dataframe of responses to the Bain Survey questions with model values and a resulting Bain Segment
    Input: 
        - res: A dataframe of responses to Bain Survey quesitons with the following columns:
            + 'UID'
            + 'I always choose higher end hotels'
            + 'On vacation I pride myself on discovering new places and things my friends have not seen before'
            + 'I choose more expensive hotels and accommodations to make sure I get the best service and amenities'
            + 'It is important for me to be in a luxurious setting and to be pampered on vacation'
            + 'I like to immerse myself in the local culture when on vacation'
            + 'The difference between luxury and standard hotels just is not big enough to justify the cost'
            + 'I always pay more for a higher class of room on my vacations'
            + 'I would rather spend as little as possible on accommodations so I have more money to spend on other parts of my vacation'
            + 'I think spending more for unique experiences is part of what makes a vacation special'
            + 'I like to go on vacations that are off the beaten path'
    Output: 
        - res: An enriched res dataframe with added columns:
            + One column per Bain segment, where each value is the resulting value of the regression model
            + 'Assigned Segment': The assigned segment is the segment which has the maximum resulting regression value
            + 'Assigned Segment (Model Value)': The maximum model value which was used to assign the segment
    """
    #Libarary load
    import numpy as np
    import pandas as pd
    
    #Bain model - hardcoded from past Bain research
    #- mod: A dataframe of the Bain model table, which includes:
    #        + Columns for the Survey Questions from res, 
    #        + Constant column
    #        + One row per segment, the segment choices are:
    #            * 'Cheap + Comfy'
    #            * 'Cheap + Curious'
    #            * 'Curious'
    #            * 'Comfy'
    #            * 'Craver'
    #            * 'Caviar'
    
    mod = pd.DataFrame.from_dict({
           'Respondent': {0: 'Cheap + Comfy', 1: 'Cheap + Curious', 2: 'Curious', 3: 'Comfy', 4: 'Craver', 5: 'Caviar'}, 
           'I always choose higher end hotels': {0: 3.48, 1: 2.77, 2: 4.866, 3: 5.229, 4: 6.956, 5: 6.717}, 
           'On vacation I pride myself on discovering new places and things my friends have not seen before': {0: 2.552, 1: 4.19, 2: 3.886, 3: 2.538, 4: 3.885, 5: 3.148}, 
           'I choose more expensive hotels and accommodations to make sure I get the best service and amenities': {0: 2.732, 1: 1.959, 2: 4.075, 3: 4.544, 4: 5.557, 5: 6.001}, 
           'It is important for me to be in a luxurious setting and to be pampered on vacation': {0: 2.225, 1: 1.579, 2: 3.129, 3: 3.857, 4: 5.598, 5: 5.022}, 
           'I like to immerse myself in the local culture when on vacation': {0: 3.606, 1: 5.185, 2: 4.859, 3: 3.747, 4: 5.017, 5: 4.459}, 
           'The difference between luxury and standard hotels just is not big enough to justify the cost': {0: 4.344, 1: 4.576, 2: 3.847, 3: 3.701, 4: 4.127, 5: 2.815}, 
           'I always pay more for a higher class of room on my vacations': {0: 2.23, 1: 1.694, 2: 3.119, 3: 3.786, 4: 4.806, 5: 4.864}, 
           'I would rather spend as little as possible on accommodations so I have more money to spend on other parts of my vacation': {0: 2.988, 1: 3.588, 2: 2.907, 3: 2.755, 4: 3.286, 5: 1.919}, 
           'I think spending more for unique experiences is part of what makes a vacation special': {0: 3.53, 1: 4.606, 2: 4.568, 3: 3.776, 4: 4.932, 5: 4.444}, 
           'I like to go on vacations that are off the beaten path': {0: 1.874, 1: 2.82, 2: 2.83, 3: 1.939, 4: 2.628, 5: 2.085}, 
           'Constant': {0: -41.736, 1: -58.956, 2: -66.499, 3: -57.042, 4: -96.451, 5: -78.828}
    })
    
    #Input Validations
    #10 Survey questions must be *exactly* the same in response and model DFs
    if np.intersect1d(mod.columns, res.columns).shape[0] == 12: 
        raise Exception("The 10 Survey questions must be *exactly* the same in the res and mod DFs.")
    
    
    #Helper function
    def value_extract(mod, col, seg):
        """
        Purpose: Extract a single cell coefficient from the model table, based on question and segment
        Input: 
            - mod: Model table
            - col: Column in model table (question)
            - seg: Bain segment
        Output: Single cell coefficient from the model table
        """
        return mod[col][mod["Respondent"]==seg].reset_index(drop=True)[0]

    #Temp assignment (values will be overriden)
    res.loc[:,'Assigned Segment'] = ""
    res.loc[:,'Assigned Segment (Model Value)'] = 0
    
    #Main loop
    #Loop through Bain Segments
    for i in mod.Respondent:
        res.loc[:,i] = value_extract(mod, 'Constant', i)
        
        #Loop through Bain survey questions
        for a in mod.columns[1:11]:
            res.loc[:,i] = res.loc[:,i] + (value_extract(mod, a, i) * res[a])

        #Assign segment if resulting model value is the maximum resulting value
        res.loc[:,'Assigned Segment'] = np.where(
            res['Assigned Segment (Model Value)'] >= res[i], res['Assigned Segment'], i)
        res.loc[:,'Assigned Segment (Model Value)'] = np.where(
            res['Assigned Segment'] == i, res[i], res['Assigned Segment (Model Value)'])
    
    #Return the enriched res dataframe
    return res