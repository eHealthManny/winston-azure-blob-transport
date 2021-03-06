Promise = require('bluebird')
moment = require('moment')

###*
# Fetches the latest blob in the container with the specefied prefix
# continuationToken - is a special parameter returned by the blob storage sdk. If passed in, then a previous search will be continued.
#                   ensure that you pass the same parameters as the original search when using the continuation token
###

getLatestBlob = (service, containerName, searchString, searchOptions, continuationToken) ->
    # promisify certain functions
    service.listContainersSegmentedWithPrefix = Promise.promisify(service.listContainersSegmentedWithPrefix)
    service.listBlobsSegmentedWithPrefix = Promise.promisify(service.listBlobsSegmentedWithPrefix)
    realBlobResults = []
    # valid options are documented here: https://github.com/Azure/azure-storage-node/blob/master/lib/services/blob/blobservice.core.js#L1184
    azureSearchOptions = {}
    # set max blob results.
    if searchOptions and searchOptions.maxResults
        azureSearchOptions.maxResults = searchOptions.maxResults
    # set exact match container search
    if searchOptions and searchOptions.exactContainerName
        azureSearchOptions.exactContainerName = true
    else
        azureSearchOptions.exactContainerName = false
    # if no blob search paramter was search everything
    if !searchString
        searchString = ''
    # use the microsoft provided service to list all blobs in the current container with our prefix.
    return service.listBlobsSegmentedWithPrefix(containerName, searchString, continuationToken, azureSearchOptions)

###*
# Generates a string used to search for blobs. Our blob names look like this 
# GAPI-2018-07-27T17:27:37.166Z 
# 
# This function will generate this type of string given a date and booleans to determine how specefic ex:
# generateBlobString('GAPI',{2018-07-27T17:27:37.166Z}, false, false) // "GAPI-2018"
# generateBlobString('GAPI', {2018-07-27T17:27:37.166Z}, true, false) // "GAPI-2018-07"
# generateBlobString('GAPI', {2018-07-27T17:27:37.166Z}, true, true)  // "GAPI-2018-07-27"
# 
# @param {appPrefix} appPrefix	The prefix of the app to use in the search string
# @param {momentDate} date The date used to generate our search string 
# @param {Boolean} useMonth If true the resulting search string will contain the month from our date
# @param {Boolean} useDay  If true the resulting search string will contain the day from our date
###

generateBlobSearchString = (appPrefix, date, useMonth, useDay) ->
    dateSearchSubString = ''
    # we may want to consider adding a drill down option for time as well.
    dateSearchSubString += 'YYYY'
    if useMonth
        dateSearchSubString += '-MM'
    if useDay
        dateSearchSubString += '-DD'
    appPrefix + '-' + date.format(dateSearchSubString)

###*
#	Finds the most recent blob logs for a given blob prefix. This function assumes that the blobs are named with an iso 8601 timestamp
        ex: 2018-07-28T14:31:52+00:00
# @param {*} containerPrefix The exact container name to match on.
# @param {*} blobPrefix - A blob prefix to restrict search to.
# @param {*} limit - The number of blobs to return from the search.
###

searchLatestBlob = (service, containerPrefix, blobPrefix, limit) ->
    # the result of our search
    result = undefined
    # start with today
    curDateSearch = moment().add(3, 'month')
    
    # do we need to drill down to the day level?
    day = false
    
    # do we need to drill down to the month level?
    month = false

    # do we need to start paging through results?
    useContinuation = false;

    # make our search (limit results to 100)
    recursiveSearch = (continuationToken)->
        # This is the substring of the date portion we will use to search for blobs ex: 2018 or 2018-05 OR 2018-05-21. 
        nextSearch = generateBlobSearchString(blobPrefix, curDateSearch, month, day)
        getLatestBlob(service, containerPrefix, nextSearch, {maxResults: 100, exactContainerName: true}, continuationToken).then (nextResult) ->
            # Sort the blobs in descending order.
            nextResult.entries.sort (a, b) ->
                if b.name < a.name
                    return -1
                if b.name > a.name
                    return 1
            # the next enclosing if will set this to something if we find a result (or give up)
            result = undefined;
            
            # results were found with the last search. Decide if we need to drill down on the next search or if we found the latest blob.
            if nextResult? and nextResult.entries.length > 0
                if  !nextResult.continuationToken?
                    # if there are results and no continuation token we found the latest blob.
                    result = nextResult.entries[0]
                else
                    # if there are results and continuation token = true, there are more results. On next search restrict our timeframe
                    if !month 
                        month = true 
                    else if !day 
                        day = true
                    else 
                        useContinuation = true
            # no results were found so start moving back in time to search
            else
                if day is true
                    curDateSearch.subtract 1, 'day'
                else if month is true
                    curDateSearch = curDateSearch.subtract(1, 'month').endOf('month')
                else
                    curDateSearch.subtract(1, 'year').endOf 'year'

            if result? or (moment().year() - curDateSearch.year() >= 20)
                # result was found OR we searched longer than 20 years
                return result
            else 
                # if we need to start drilling down into paged results send the continuation token (if any)
                nextContuationToken = if useContinuation? then nextResult.continuationToken else undefined
                return recursiveSearch(nextContuationToken);

    return recursiveSearch()

module.exports =
    getLatestBlob: getLatestBlob
    searchLatestBlob: searchLatestBlob