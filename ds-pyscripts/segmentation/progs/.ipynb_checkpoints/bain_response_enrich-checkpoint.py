def bain_response_enrich(res, mod):
    """
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
        - mod: A dataframe of the Bain model table, which includes:
            + Columns for the Survey Questions from res, 
            + Constant column
            + One row per segment, the segment choices are:
                * 'Cheap + Comfy'
                * 'Cheap + Curious'
                * 'Curious'
                * 'Comfy'
                * 'Craver'
                * 'Caviar'
    Output: An enriched res dataframe with added columns:
        - One column per Bain segment, where each value is the resulting value of the regression model
        - 'Assigned Segment': The assigned segment is the segment which has the maximum resulting regression value
        - 'Assigned Segment (Model Value)': The maximum model value which was used to assign the segment
    """
    
    #Input Validations
    #Only use Bain Segments
    if np.sum(np.array(['Cheap + Comfy', 'Cheap + Curious', 'Curious', 'Comfy', 'Craver','Caviar']) != 
           mod.Respondent.values) != 0:
        raise Exception("Only use the predefined Bain segments ('Cheap + Comfy', 'Cheap + Curious', 'Curious', 'Comfy', 'Craver','Caviar') in mod.")
    
    #10 Survey questions must be *exactly* the same in response and model DFs
    if np.sum(mod.columns[1:11] != res.columns[1:11]) != 0:
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