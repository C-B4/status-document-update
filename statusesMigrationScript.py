#!/usr/bin/python3.4

import argparse
import datetime
import json
from uuid import UUID
from cassandra.cluster import Cluster
from cassandra.util import datetime_from_uuid1, min_uuid_from_time


SELECT_ALL_USERS_IDS_QUERY = "SELECT user_id FROM unifiedpush_server.users_by_application where push_application_id=%s and month in (1,2,3,4,5,6,7,8,9,10,11,12);"
SELECT_IS_RESPONSE_EXIST = "SELECT * FROM unifiedpush_server.documents WHERE database = %s and user_id = %s and push_application_id = %s and document_id = %s limit 1;"
INSERT_DOCUMENT_QUERY = "INSERT INTO unifiedpush_server.documents (push_application_id, database, user_id, snapshot, content, content_type, document_id) VALUES(%s, %s, %s, %s, %s, %s, %s);"
# changing this query does not change anything, it is here only for use of printing to the console
# the execution is done with string concat manually (for this query only) due to technical limitation
SELECT_ALL_STATUS_DOCUMENTS_QUERY = "SELECT content FROM unifiedpush_server.documents where push_application_id=? and database = 'STATUS' and user_id IN (?);"



def parse_arguments():
    parser = argparse.ArgumentParser(description='Migrate statuses documents into responses documents in unified push server',
                                     formatter_class=argparse.RawTextHelpFormatter)

    # parser.add_argument(
    #     '--application-ids',
    #     nargs='+',
    #     type=UUID,
    #     dest='applicationIds',
    #     help=''
    # )

    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        dest='verbose',
                        help='delete soft alias by guid'
    )

    parser.add_argument(
        '--contact-points',
        nargs='+',
        dest='contactPoints',
        help=''
    )

    parser.add_argument(
        '--application-ids-path',
        type=str,
        dest='applicationIdsPath',
        help='text file path that contains all application ids, each application id should be on a new line'
    )

    parser.add_argument(
        '--prev-snapshot-file-path',
        default='./prevSnapshot.txt',
        type=str,
        dest='prevSnapshotFilePath',
        help='path to text file (not have to exist, BUT the directory should be), by default the file will be created in the directory where this python script is. the file will contain AUTOMATICLY the latest snapshot run (to prevent converting twice the same documents)'
    )

    parser.format_help()
    args = parser.parse_args()
    return args


class ArgUtil:
    args = None

    @staticmethod
    def setArgs(args):
        ArgUtil.args = args

    @staticmethod
    def getArgs():
        return ArgUtil.args


def main():
    args = parse_arguments()
    ArgUtil.setArgs(args)

    newMostResentSnapshot = min_uuid_from_time(datetime.datetime.now().timestamp())
    prevSnapshot = parsePrevSnapshot()
    print('prevSnapshot:'+str(datetime_from_uuid1(prevSnapshot)))
    print('newMostResentSnapshot:' + str(datetime_from_uuid1(newMostResentSnapshot)))

    applicationsPath = args.applicationIdsPath
    applicationUUIIdsFromFile = parseApplicationUUIDsFromFile(applicationsPath)
    if args.verbose:
        appIdsStringList = [str(appUUID) for appUUID in applicationUUIIdsFromFile]
        print('Application UUIDs from file: ' + str(appIdsStringList))

    contactPoints = args.contactPoints
    cluster = Cluster(contactPoints)
    totalStatusDocumentSplited = 0
    totalResponsesCreated = 0
    try:
        session = cluster.connect()
        print('connecting to cluster: [' + ', '.join(contactPoints) + ']  ...')
        session.execute('use unifiedpush_server')

        for appUUID in applicationUUIIdsFromFile:
            totalResponsesCreated, totalStatusDocumentSplited = \
                workOnApplication(appUUID, session, totalResponsesCreated, totalStatusDocumentSplited, prevSnapshot)

        updateMostResentSnapshot(newMostResentSnapshot)

    finally:
        print('Total number of STATUS document found: ' + str(totalStatusDocumentSplited))
        print('Total number of RESPONSE document created: ' + str(totalResponsesCreated))
        print('closing cluster connections...')
        cluster.shutdown()


def updateMostResentSnapshot(newMostResentSnapshot):
    with open(ArgUtil.args.prevSnapshotFilePath, 'w') as the_file:
        the_file.write(str(newMostResentSnapshot))


def parseApplicationUUIDsFromFile(applicationsPath):
    with open(applicationsPath, 'r') as f:
        content = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    result = [UUID(x.strip()) for x in content]
    return result


def parsePrevSnapshot():
    try:

        with open(ArgUtil.args.prevSnapshotFilePath, 'r') as f:
            content = f.readlines()
            if content[0].strip() is not None:
                return UUID(content[0].strip())
    except:
        return min_uuid_from_time(0) # if not exist, take uuid with epoch beggining time (1/1/1970)


def workOnApplication(appUUID, session, totalResponsesCreated, totalStatusDocumentSplited, prevSnapshot):
    userIds = getAllUsersIds(session, appUUID)
    userIdsAsString = ','.join(userIds)
    statusDocuments = getAllStatusDocuments(session, appUUID, userIdsAsString, prevSnapshot)
    userToMaxSnapshot = {}
    userToMinSnapshot = {}

    # grouping by deploy id and user id.
    # both userToMaxSnapshot and userToMinSnapshot are gouping same keys
    # value of userToMaxSnapshot is the document with the highest snapshot (uuid) in the group
    # value of userToMinSnapshot is the document with the lowest snapshot (uuid) in the group
    for doc in statusDocuments:
        # the document_id in STATUS documents is the deploy id , so no need to read the deploy_id from json
        groupbyKey = (doc.document_id, doc.user_id)
        prevDoc = userToMaxSnapshot.get(groupbyKey)
        if prevDoc is None or prevDoc.snapshot.time < doc.snapshot.time:
            userToMaxSnapshot[groupbyKey] = doc

        if prevDoc is None:
            userToMinSnapshot[groupbyKey] = doc.snapshot
        if prevDoc is not None and doc.snapshot.time < prevDoc.snapshot.time:
            userToMinSnapshot[groupbyKey] = doc.snapshot
    for userIdDeployIdTuple, statusDocument in userToMaxSnapshot.items():
        userUUID = userIdDeployIdTuple[1]
        responsesCreated = splitConvertToResponsesAndPutIfAbsent(session, userUUID, statusDocument.content,
                                                                 userToMinSnapshot[userIdDeployIdTuple],
                                                                 statusDocument.snapshot, appUUID)
        totalResponsesCreated += responsesCreated
        totalStatusDocumentSplited += 1
    return totalResponsesCreated, totalStatusDocumentSplited


def getAllUsersIds(session, applicationId):
    print('executing test query: ' + SELECT_ALL_USERS_IDS_QUERY + ' ...')
    rows = session.execute(SELECT_ALL_USERS_IDS_QUERY, [applicationId])
    result = [user.user_id.__str__() for user in rows]
    return result


def getAllStatusDocuments(session, applicationUUId, userIds, prevSnapshot):
    print('executing test query: ' + SELECT_ALL_STATUS_DOCUMENTS_QUERY + ' ...')
    rows = session.execute(
        "SELECT user_id, snapshot, content, document_id FROM unifiedpush_server.documents where push_application_id=" + str(applicationUUId) + " and database = 'STATUS' and user_id IN (" + userIds + ") and snapshot > " + str(prevSnapshot) + ";")
    return rows.current_rows


def splitConvertToResponsesAndPutIfAbsent(session, userUUID, content, minSnapshotUUID, maxSnapshotUUID, appUUID):

    minSnapshotDocument = datetime_from_uuid1(minSnapshotUUID)
    maxSnapshotDocument = datetime_from_uuid1(maxSnapshotUUID)
    now = datetime.datetime.now()
    dateDiff = now - minSnapshotDocument

    isPassed30days = dateDiff.days > 30

    isLogVerbose = ArgUtil.args.verbose
    if isLogVerbose:
        print('-----------------------------------------------------------------------------')
        print('max snapshot date : ' + maxSnapshotDocument.strftime('%d/%b/%Y'))
        print('min snapshot date : ' + minSnapshotDocument.strftime('%d/%b/%Y'))
        print('now : ' + now.strftime('%d/%b/%Y'))
        print('Date diff from now : days = %d' % (dateDiff.days))
        print('isPassed30days : ' + str(isPassed30days))

    data = json.loads(content)
    version = data['version']
    deployId = data['deployId']
    alias = data['alias']
    actions = data['actions']
    totalResponsesDocumentCreated = 0
    for actionId, statesMap in actions.items():
        statesList = statesMap['states']

        if isPassed30days:
            lifeCycle = 'EXPIRED'
        else:
            lifeCycle = 'ACTIVE'

        for state in statesList:
            update_date = state['updateDate']
            try:
                remark = state['remark']
            except:
                remark = ""
            status_state = state['state']
            if status_state == 'done':
                status_state = 'DONE'
            else:
                status_state = 'NOT_DONE'
            reasonId = state['reasonId']

            responseObject = {
                'version': version,
                'alias': alias,
                'deployId': deployId,
                'taskId': actionId,
                'reasonId': reasonId,
                'state': status_state,
                'remark': remark,
                'viewed': True,
                'updateDate': update_date,
                'lifeCycle': lifeCycle,
            }
            totalResponsesDocumentCreated += 1
            if isLogVerbose:
                print(json.dumps(responseObject, sort_keys=True, indent=4, separators=(',', ': ')))

            putResponseIfAbsent(session, responseObject, userUUID, appUUID, maxSnapshotUUID)

    return totalResponsesDocumentCreated


def putResponseIfAbsent(session, responseObject, userUUID, appUUID, maxSnapshotUUID):
    # check if RESPONSE document already exist
    isResponseExist = isResponseDocumentExist(session, 'RESPONSE', userUUID, appUUID, responseObject['taskId'])
    if ArgUtil.args.verbose:
        print('isResponseAlreadyExist: ' + str(isResponseExist))

    if not isResponseExist:
        newTimeStampUUID = min_uuid_from_time(datetime.datetime.now().timestamp())
        print('inserting new response: appId=' + str(appUUID) + ', database=RESPONSE, ' + 'userId=' + str(userUUID) + ', snapshot=' + str(newTimeStampUUID) + ', responseObject=' + json.dumps(responseObject) + ', document_id=' + responseObject['taskId'])
        session.execute(INSERT_DOCUMENT_QUERY, [appUUID, 'RESPONSE', userUUID, newTimeStampUUID, json.dumps(responseObject),"application/json", responseObject['taskId']])


def isResponseDocumentExist(session, database, userId, appId, documentId):
    rows = session.execute(SELECT_IS_RESPONSE_EXIST, parameters=[database,userId,appId,documentId])
    return len(rows.current_rows) > 0


if __name__ == "__main__":
    main()

