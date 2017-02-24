import sqlparse
import re
from pydrill.client import PyDrill

_FLOAT_FUNCTIONS = [
    'CBRT',
    'DEGREES',
    'E',
    'EXP',
    'LOG',
    'MOD',
    'PI',
    'POW',
    'RADIANS'
    'RAND',
    'ROUND',
    'TRUNC',
    'EXTRACT',
    'DATE_PART',
    'AVG',
    'CUME_DIST',
    'PERCENT_RANK'
]

_INT_FUNCTIONS = [
    'SIGN',
    'CHAR_LENGTH',
    'POSITION',
    'STRPOS',
    'NTILE'
]
_BIG_INT_FUNCTIONS = [
    'COUNT',
    'DENSE_RANK',
    'RANK',
    'ROW_NUMBER'

]

def get_column_types( query ):

    drill = PyDrill(host='localhost', port=8047)
    data = drill.query(query)
    columns = data.columns
    types = {}

    formattedQuery = sqlparse.format(query, reindent=True, keyword_case='upper')
    formattedQuery = formattedQuery.split('\n')
    print( sqlparse.format(query, reindent=True, keyword_case='upper'))
    inSelect = False
    inSubquery = False
    inFromClause = False
    fields = []

    fieldRegex = r'\s{7}\S'
    fieldSubquery = r'\s'
    subqueryFieldPattern = r'\s{2,3}\S'
    subqueryField = ""

    fromClause = ""
    functionPattern = r'\s+(\S+)\('

    fieldCount = 0
    for line in formattedQuery:
        functionMatchObject = re.match(functionPattern, line)

        if line.startswith( 'SELECT'):
            inSelect = True
            line = line.replace( 'SELECT', '')

            line = line.strip()
            #remove trailing comma
            if len(line) > 0:
                if line[-1:] == ",":
                    line = line[:-1]
            fields.append(line)

        # If the line is a function, assign the correct return type
        elif inSelect and inFromClause == False and functionMatchObject:
            print( "FieldCount: " + str(fieldCount ) + " " + line)
            functionCandidate = functionMatchObject.group(1)
            functionCandidate = functionCandidate.upper()
            if functionCandidate in _BIG_INT_FUNCTIONS:
                types[columns[fieldCount]] = "bigint"

            elif functionCandidate in _INT_FUNCTIONS:
                types[columns[fieldCount]] = "integer"

            elif functionCandidate in _FLOAT_FUNCTIONS:
                types[columns[fieldCount]] = "float"

            else:
                types[columns[fieldCount]] = "varchar"

            fieldCount += 1
            continue

        # Case for a regular field
        elif inSelect == True and re.match(fieldRegex, line):
            line = line.strip()
            # remove trailing comma from field name
            if len(line) > 0:
                if line[-1:] == ",":
                    line = line[:-1]
            fields.append(line)

        elif inSelect == True and line.startswith('FROM'):
            inSelect = False
            inFromClause = True
            if inSubquery:
                fields.append( subqueryField )
                inSubquery = False
            else:
                fromClause = fromClause + " " + line.strip()

        elif inFromClause == True and (line.startswith( 'WHERE') or line.startswith('GROUP') or line.startswith('ORDER') or line.startswith('HAVING')):
            inFromClause = False
            inSelect = False

        elif re.match(subqueryFieldPattern, line) and inSubquery == False and inFromClause == False:
            inSubquery = True
            subqueryField = line.strip()

        elif inSubquery == True:
            subqueryField = subqueryField + " " + line.strip()
            if line.endswith( ','):
                inSubquery = False
                fields.append(subqueryField)
                subqueryField = ""

        elif inSubquery == True and line == False:
            inSubquery = False
            fields.append(subqueryField)
            subqueryField = ""

        elif inFromClause == True:
            fromClause = fromClause + " " + line.strip()

        fieldCount += 1

    typeQuery = "SELECT"
    fieldCount = 0
    aliasPattern = r'AS\s`?[a-zA-Z_][a-zA-Z0-9-_$` ]*$'
    for field in fields:
        if re.search(aliasPattern, field):
            field = re.sub(aliasPattern, '', field)

        if fieldCount > 0:
            typeQuery += ","
        typeQuery = typeQuery + " " + field + " AS " + columns[fieldCount] + ", typeof( " + field + ") AS " + columns[fieldCount] + "_type"
        fieldCount += 1

    typeQuery += fromClause
    typeQuery += " LIMIT 1"
    typeQuery = sqlparse.format(typeQuery, reindent=True, keyword_case='upper')

    print ( typeQuery)
    fieldQueryResult = drill.query(typeQuery).to_dataframe()
    tempTypes = fieldQueryResult.T.to_dict()[0]

    for column in columns:
        if column not in types.keys():
            types[ column ] = tempTypes[ column + "_type"]

    print( types )
    return types


query2 = "SELECT uadata.ua.AgentNameVersion AS Browser, COUNT( * ) AS BrowserCount FROM (    SELECT parse_user_agent( columns[0] ) AS ua    FROM dfs.drillworkshop.`csv/user-agents.csv` ) AS uadata GROUP BY uadata.ua.AgentNameVersion ORDER BY BrowserCount DESC LIMIT 10"
#create_type_query(query1)

get_column_types(query2)

#get_column_types("select Style, beds from dfs.test.`comps.xlsx`")


