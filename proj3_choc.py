import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

# Creates a database called yourlastnamefirstname_big10.sqlite
def drop_db():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    # Code below provided for your convenience to clear out the big10 database
    # This is simply to assist in testing your code.  Feel free to comment it
    # out if you would prefer
    statement = '''DROP TABLE IF EXISTS 'Bars';'''

    # print(statement)
    cur.execute(statement)
    conn.commit()
    # print(cur.rowcount)

    statement = 'DROP TABLE IF EXISTS Countries;'
    cur.execute(statement)

    conn.commit()
    conn.close()


def create_countries():
    # Connect to big10 database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    #create Countries TABLE
    statement = '''
        CREATE TABLE 'Countries' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT NOT NULL,
            'Alpha3' TEXT ,
            'EnglishName' TEXT ,
            'Region' TEXT ,
            'Subregion' TEXT ,
            'Population' INTEGER ,
            'Area' REAL
            );
    '''
    cur.execute(statement)
    #populate countries table
    json_data=open(COUNTRIESJSON, encoding='utf8').read()
    data=json.loads(json_data)
    #print(len(data))
    #none_counter=0
    for i in data:
        EnglishName=i['name']
        Alpha2=i['alpha2Code']
        Alpha3=i['alpha3Code']
        Region=i['region']
        # if Region not in ['Americas', 'Africa', "Oceania", 'Asia']:
        #     print(Region)
        #     none_counter+=1
        Subregion=i['subregion']
        Population=i['population']
        Area=i['area']
        insertion=(None, Alpha2, Alpha3,EnglishName, Region, Subregion, Population, Area)
        statement ='INSERT INTO "Countries" '
        statement +='VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)
    conn.commit()
    conn.close()
    #print(none_counter)
    return


def populate_bars():
    # Connect to big10 database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    #create Bars TABLE
    statement = '''
        CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT ,
            'SpecificBeanBarName' TEXT ,
            'REF' TEXT,
            'ReviewDate' TEXT,
            'CocoaPercent' REAL,
            'CompanyLocationId' INTEGER,
            'Rating' REAL,
            'BeanType' TEXT ,
            'BroadBeanOriginId' INTEGER,
            FOREIGN KEY(CompanyLocationId) REFERENCES Countries(Id),
            FOREIGN KEY(BroadBeanOriginId) REFERENCES Countries(Id)
            );
    '''
    cur.execute(statement)
    #populate bars into database
    bars_list=[]
    with open('flavors_of_cacao_cleaned.csv', encoding='utf8') as csvDataFile:
        csvReader=csv.reader(csvDataFile)
        next(csvReader)
        for row in csvReader:
            bars_list.append(row)

    for bar in bars_list:
        BroadBeanOriginId=None
        Company=bar[0]
        SpecificBeanBarName=bar[1]
        REF=bar[2]
        ReviewDate=bar[3]
        #change percentage to number
        raw_CocoaPercent=bar[4]
        CocoaPercent=float(raw_CocoaPercent.strip('%'))/100
        #obtain company name
        CompanyLocationName=bar[5]#update with id from countries table
        # print(CompanyLocationName)
        statement='SELECT Id from Countries WHERE EnglishName= "'+str(CompanyLocationName)+'" '
        # print(statement)
        cur.execute(statement)
        for i in cur:
            CompanyLocationId=i[0]

        Rating=bar[6]
        BeanType=bar[7]
        #obtain bean orgin Id
        BroadBeanOriginName=bar[8]#update with id from countries table
        statement='SELECT Id from Countries WHERE EnglishName = "'+str(BroadBeanOriginName)+'" '
        cur.execute(statement)

        i=cur.fetchone()
        #if bar[8] == "Unknown":
        #    print(i)
        if i is not None:
            BroadBeanOriginId=i[0]

        insertion=(None, Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocationId, Rating, BeanType, BroadBeanOriginId)
        # print(insertion)
        statement ='INSERT INTO "Bars" '
        statement +='VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)


    conn.commit()
    conn.close()
    return

# Part 2: Implement logic to process user commands

#bars function commands
def bars_query(kwargs):
    #null values

    sellcountry=None,
    sourcecountry=None,
    sellregion=None,
    sourceregion=None,
    order=None,
    top=10,
    bottom=None
    variables=['bars','ratings','sellcountry', 'sourcecountry', 'sellregion', 'sourceregion', 'order', 'top','bottom','cocoa']
    #needed staements
    main_statement='SELECT b.SpecificBeanBarName, b.Company, c.EnglishName, b.Rating, b.CocoaPercent, r.EnglishName FROM Bars as b JOIN Countries as c ON c.Id=b.CompanyLocationId JOIN Countries as r ON b.BroadBeanOriginId=r.Id '

    input_list=kwargs.keys()

    if 'bars' in input_list and len(input_list)>1:
        if set(input_list).issubset(variables):
            pass
        else:
            invalid_print='You have entered invalid parameters for the '+"'Bars' function. Please type "+"'help' for more information."
            return print(invalid_print)



    first_input_counter=0
    cocoa_counter=0
    rating_counter=0
    top_counter=0
    bottom_counter=0
    statement=main_statement
    for name, value in kwargs.items():
        if name in ['sellcountry', 'sourcecountry', 'sellregion', 'sourceregion']:

            if name == "sellcountry":
                sellcountry=value
                sell_country_statement='c.Alpha2 = "'+str(sellcountry)+'" '
                if first_input_counter >0:
                    statement +='AND ' +sell_country_statement
                else:
                    statement+='WHERE '+sell_country_statement
                first_input_counter +=1
                #print(statement)
            if name=="sourcecountry":
                sourcecountry=value
                source_country_statement='r.Alpha2 = "'+str(sourcecountry)+'" '
                if first_input_counter >0:
                    statement +='AND ' +source_country_statement
                else:
                    statement+='WHERE '+source_country_statement
                first_input_counter +=1
                #print(statement)
            if name=="sellregion":
                sellregion=value
                sell_region_statement='c.Region = "'+str(sellregion)+'" '
                if first_input_counter >0:
                    statement +='AND ' +sell_region_statement
                else:
                    statement+='WHERE '+sell_region_statement
                first_input_counter +=1
                #print(statement)
            if name=="sourceregion":
                sourceregion=value
                source_region_statement='r.Region = "'+str(sourceregion)+'" '
                if first_input_counter >0:
                    statement +='AND ' +source_region_statement
                else:
                    statement+='WHERE '+source_region_statement
                first_input_counter +=1
                #print(statement)
        if name=="cocoa":
            order_statement='ORDER BY b.CocoaPercent '
            statement +=''+order_statement
            cocoa_counter +=1
            rating_counter +=1
        if cocoa_counter==0 and rating_counter==0 and first_input_counter>0:
            statement +='ORDER BY b.rating '
            rating_counter +=1
        if rating_counter==0 and "cocoa" not in input_list and name!="bars":
            statement +='ORDER BY b.rating '
            rating_counter +=1
            #print(statement)
        if name=="top":
            top=value
            top_limit_statement='DESC LIMIT '+str(top)+' '
            statement +=''+top_limit_statement
            top_counter+=1
            #print(statement)
        if name=="bottom":
            bottom=value
            bottom_limit_statement='ASC LIMIT '+str(bottom)+ ' '
            statement +=''+bottom_limit_statement
            bottom_counter+=1



    if cocoa_counter==0 and rating_counter==0:
        statement +='ORDER BY b.rating '
    if bottom_counter==0 and top_counter==0:
        statement+='DESC LIMIT 10 '

    #print(statement)

    # Connect to big10 database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    cur.execute(statement)
    results=cur.fetchall()
    str_results=[str(i) for i in results]
    #print(str_results)
    col_width=max(len(str(word)) for row in results for word in row)
    #print(col_width)
    for i in results:
        barname=i[0]
        if len(barname)>12:
            barname=barname[:12]+'...'
        company=i[1]
        if len(company)>12:
            company=company[:12]+'...'
        complocation=i[2]
        if len(complocation)>12:
            complocation=complocation[:12]+'...'
        rating=i[3]
        cocoa_per=i[4]
        beanorigin=i[5]
        if len(beanorigin)>12:
            beanorigin=beanorigin[:12]+'...'

        cocoa_format=str(round((cocoa_per*100),0))[:2]

        message=(
            f"{barname.ljust(15)} "
            f"{company.ljust(15)} "
            f"{complocation.ljust(15)} "
            f"{str(rating).ljust(10)} "
            f"{cocoa_format+'%'.ljust(10)} "
            f"{beanorigin.ljust(15)} "

        )
        print(message)

    conn.close()

    return results
# bars_query(ratings='')


def company_query(kwargs):
    variables=['companies','country', 'region', 'ratings','cocoa','bars_sold','top','bottom']

    main_statement='SELECT b.Company, c.EnglishName, '
    avg_rating_statement='avg(b.rating) '
    avg_cocoa_staement='avg(b.CocoaPercent) '
    bars_sold_statement='count(b.SpecificBeanBarName) '
    join_statement='FROM Bars as b JOIN Countries as c ON b.CompanyLocationId = c.Id '
    group_statement='GROUP BY b.Company HAVING count(b.SpecificBeanBarName) >4 '
    order_cocoa_statement='ORDER BY avg(b.CocoaPercent) '
    order_rating_statement='ORDER BY avg(b.rating) '
    order_bars_statement='ORDER BY count(SpecificBeanBarName) '

    input_list=kwargs.keys()

    if 'companies' in input_list and len(input_list)>1:
        if set(input_list).issubset(variables):
            pass
        else:
            invalid_print='You have entered invalid parameters for the '+"'companies' function. Please type "+"'help' for more information."
            return print(invalid_print)


    first_input_counter=0
    cocoa_counter=0
    rating_counter=0
    bars_counter=0
    top_counter=0
    bottom_counter=0

    statement=main_statement

    if 'ratings' in input_list:
        statement +=avg_rating_statement+join_statement
        rating_counter +=1
    if 'cocoa' in input_list:
        statement +=avg_cocoa_staement+join_statement
        cocoa_counter+=1
    if 'bars_sold' in input_list:
        statement+=bars_sold_statement+join_statement
        bars_counter+=1

    for name, value in kwargs.items():
        if name in ['country', 'region']:
            if name == "country":
                country=value
                country_statement='c.Alpha2 = "'+str(country)+'" '
                if first_input_counter >0:
                    statement +='AND ' +country_statement
                else:
                    statement+='WHERE '+country_statement
                first_input_counter +=1
                #print(statement)
            if name=="region":
                region=value
                region_statement='c.Region = "'+str(region)+'" '
                if first_input_counter >0:
                    statement +='AND ' +region_statement
                else:
                    statement+='WHERE '+region_statement
                first_input_counter +=1
        if name in ['top','bottom']:
            if name =='top':
                if rating_counter>0:
                    top=value
                    statement+=group_statement+order_rating_statement+'DESC LIMIT '+str(top)+' '
                    top_counter +=1
                if cocoa_counter>0:
                    top=value
                    statement+=group_statement+order_cocoa_statement+'DESC LIMIT '+str(top)+' '
                    top_counter +=1
                if bars_counter>0:
                    top=value
                    statement+=group_statement+order_bars_statement+'DESC LIMIT '+str(top)+' '
                    top_counter +=1
            if name=='bottom':
                if rating_counter>0:
                    bottom=value
                    statement+=group_statement+order_rating_statement+'ASC LIMIT '+str(bottom)+' '
                    bottom_counter +=1
                if cocoa_counter>0:
                    bottom=value
                    statement+=group_statement+order_cocoa_statement+'ASC LIMIT '+str(bottom)+' '
                    bottom_counter +=1
                if bars_counter>0:
                    bottom=value
                    statement+=group_statement+order_bars_statement+'ASC LIMIT '+str(bottom)+' '
                    bottom_counter +=1
    if top_counter==0 and bottom_counter==0:
        if rating_counter>0:
            statement+=group_statement+order_rating_statement+'DESC LIMIT 10 '
            top_counter +=1
        elif cocoa_counter>0:
            statement+=group_statement+order_cocoa_statement+'DESC LIMIT 10 '
            top_counter +=1
        elif bars_counter>0:
            statement+=group_statement+order_bars_statement+'DESC LIMIT 10 '
            top_counter +=1
        else:
            statement+=avg_rating_statement+join_statement+group_statement+order_rating_statement+'DESC LIMIT 10 '
            rating_counter+=1


    #print(statement)
    # Connect to big10 database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    cur.execute(statement)
    results=cur.fetchall()
    # print(results)
    for i in results:
        company=i[0]
        if len(company)>12:
            company=company[:12]+'...'
        complocation=i[1]
        if len(complocation)>12:
            complocation=complocation[:12]+'...'
        agg=i[2]
        if rating_counter>0:
            agg=str(round(agg,1))
        elif cocoa_counter>0:
            agg=str(round((agg*100),0))[:2]+'%'
        else:
            agg=str(agg)

        message=(
            f"{company.ljust(15)} "
            f"{complocation.ljust(15)} "
            f"{agg.ljust(10)} "
        )
        print(message)

    conn.close()
    return results

def countries_query(kwargs):
    variables=['countries','region','sellers','sources','ratings','cocoa','top','bars_sold','bottom']
    main_statement='SELECT c.EnglishName, c.Region, '
    avg_rating_statement='avg(b.rating) '
    avg_cocoa_staement='avg(b.CocoaPercent) '
    bars_sold_statement='count(b.SpecificBeanBarName) '
    join_statement='FROM Bars as b JOIN Countries as c '
    sellers_statement='ON b.CompanyLocationId = c.Id '
    sources_statement='ON b.BroadBeanOriginId=c.Id '
    group_statement='GROUP BY c.EnglishName HAVING count(b.SpecificBeanBarName) >4 '
    order_cocoa_statement='ORDER BY avg(b.CocoaPercent) '
    order_rating_statement='ORDER BY avg(b.rating) '
    order_bars_statement='ORDER BY count(SpecificBeanBarName) '

    input_list=kwargs.keys()

    if 'countries' in input_list and len(input_list)>1:
        if set(input_list).issubset(variables):
            pass
        else:
            invalid_print='You have entered invalid parameters for the '+"'countries' function. Please type "+"'help' for more information."
            return print(invalid_print)

    first_input_counter=0
    cocoa_counter=0
    rating_counter=0
    bars_counter=0
    top_counter=0
    bottom_counter=0
    seller_counter=0

    statement=main_statement

    if 'ratings' in input_list:
        statement +=avg_rating_statement+join_statement
        rating_counter +=1
    elif 'cocoa' in input_list:
        statement +=avg_cocoa_staement+join_statement
        cocoa_counter+=1
    elif 'bars_sold' in input_list:
        statement+=bars_sold_statement+join_statement
        bars_counter+=1
    else:
        statement +=avg_rating_statement+join_statement
        rating_counter +=1

    if 'sellers' in input_list:
        statement+=sellers_statement
        seller_counter+=1
    elif 'sources' in input_list:
        statement+=sources_statement
    else:
        statement+=sellers_statement

    for name, value in kwargs.items():
        if name=="region":
            region=value
            region_statement='c.Region = "'+str(region)+'" '
            if first_input_counter >0:
                statement +='AND ' +region_statement
            else:
                statement+='WHERE '+region_statement
            first_input_counter +=1

        if name in ['top','bottom']:
            if name =='top':
                if rating_counter>0:
                    top=value
                    statement+=group_statement+order_rating_statement+'DESC LIMIT '+str(top)+' '
                    top_counter +=1
                if cocoa_counter>0:
                    top=value
                    statement+=group_statement+order_cocoa_statement+'DESC LIMIT '+str(top)+' '
                    top_counter +=1
                if bars_counter>0:
                    top=value
                    statement+=group_statement+order_bars_statement+'DESC LIMIT '+str(top)+' '
                    top_counter +=1
            if name=='bottom':
                if rating_counter>0:
                    bottom=value
                    statement+=group_statement+order_rating_statement+'ASC LIMIT '+str(bottom)+' '
                    bottom_counter +=1
                if cocoa_counter>0:
                    bottom=value
                    statement+=group_statement+order_cocoa_statement+'ASC LIMIT '+str(bottom)+' '
                    bottom_counter +=1
                if bars_counter>0:
                    bottom=value
                    statement+=group_statement+order_bars_statement+'ASC LIMIT '+str(bottom)+' '
                    bottom_counter +=1

    if top_counter==0 and bottom_counter==0:
        if rating_counter>0:
            statement+=group_statement+order_rating_statement+'DESC LIMIT 10 '
            top_counter +=1
        elif cocoa_counter>0:
            statement+=group_statement+order_cocoa_statement+'DESC LIMIT 10 '
            top_counter +=1
        elif bars_counter>0:
            statement+=group_statement+order_bars_statement+'DESC LIMIT 10 '
            top_counter +=1
        else:
            statement+=avg_rating_statement+join_statement+group_statement+order_rating_statement+'DESC LIMIT 10 '

    print(statement)
    # Connect to big10 database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    cur.execute(statement)
    results=cur.fetchall()
    # print(results)
    for i in results:
        country=i[0]
        if len(country)>12:
            country=country[:12]+'...'
        region=i[1]
        if len(region)>12:
            region=region[:12]+'...'
        agg=i[2]
        if rating_counter>0:
            agg=str(round(agg,1))
        elif cocoa_counter>0:
            agg=str(round((agg*100),0))[:2]+'%'
        else:
            agg=str(agg)

        message=(
            f"{country.ljust(15)} "
            f"{region.ljust(15)} "
            f"{agg.ljust(10)} "
        )
        print(message)


    conn.close()
    return results

def regions_query(kwargs):
    variables=['regions','sellers','sources','ratings','cocoa','top','bars_sold','bottom']
    main_statement='SELECT c.Region, '
    avg_rating_statement='avg(b.rating) '
    avg_cocoa_staement='avg(b.CocoaPercent) '
    bars_sold_statement='count(b.SpecificBeanBarName) '
    join_statement='FROM Bars as b JOIN Countries as c '
    sellers_statement='ON b.CompanyLocationId = c.Id '
    sources_statement='ON b.BroadBeanOriginId=c.Id '
    group_statement='GROUP BY c.Region HAVING count(b.SpecificBeanBarName) >4 '
    order_cocoa_statement='ORDER BY avg(b.CocoaPercent) '
    order_rating_statement='ORDER BY avg(b.rating) '
    order_bars_statement='ORDER BY count(b.SpecificBeanBarName) '

    input_list=kwargs.keys()

    if 'regions' in input_list and len(input_list)>1:
        if set(input_list).issubset(variables):
            pass
        else:
            invalid_print='You have entered invalid parameters for the '+"'regions' function. Please type "+"'help' for more information."
            return print(invalid_print)

    first_input_counter=0
    cocoa_counter=0
    rating_counter=0
    bars_counter=0
    top_counter=0
    bottom_counter=0
    seller_counter=0

    statement=main_statement

    if 'ratings' in input_list:
        statement +=avg_rating_statement+join_statement
        rating_counter +=1
    if 'cocoa' in input_list:
        statement +=avg_cocoa_staement+join_statement
        cocoa_counter+=1
    if 'bars_sold' in input_list:
        statement+=bars_sold_statement+join_statement
        bars_counter+=1

    if 'sellers' in input_list:
        statement+=sellers_statement
        seller_counter+=1
    elif 'sources' in input_list:
        statement+=sources_statement
    else:
        statement+=sellers_statement

    for name, value in kwargs.items():
        if name in ['top','bottom']:
            if name =='top':
                if rating_counter>0:
                    top=value
                    statement+=group_statement+order_rating_statement+'DESC LIMIT '+str(top)+' '
                    top_counter +=1
                if cocoa_counter>0:
                    top=value
                    statement+=group_statement+order_cocoa_statement+'DESC LIMIT '+str(top)+' '
                    top_counter +=1
                if bars_counter>0:
                    top=value
                    statement+=group_statement+order_bars_statement+'DESC LIMIT '+str(top)+' '
                    top_counter +=1
            if name=='bottom':
                if rating_counter>0:
                    bottom=value
                    statement+=group_statement+order_rating_statement+'ASC LIMIT '+str(bottom)+' '
                    bottom_counter +=1
                if cocoa_counter>0:
                    bottom=value
                    statement+=group_statement+order_cocoa_statement+'ASC LIMIT '+str(bottom)+' '
                    bottom_counter +=1
                if bars_counter>0:
                    bottom=value
                    statement+=group_statement+order_bars_statement+'ASC LIMIT '+str(bottom)+' '
                    bottom_counter +=1

    if top_counter==0 and bottom_counter==0:
        if rating_counter>0:
            statement+=group_statement+order_rating_statement+'DESC LIMIT 10 '
            top_counter +=1
        elif cocoa_counter>0:
            statement+=group_statement+order_cocoa_statement+'DESC LIMIT 10 '
            top_counter +=1
        elif bars_counter>0:
            statement+=group_statement+order_bars_statement+'DESC LIMIT 10 '
            top_counter +=1
        else:
            statement+=avg_rating_statement+join_statement+group_statement+order_rating_statement+'DESC LIMIT 10 '
            rating_counter+=1

    #print(statement)
    # Connect to big10 database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    cur.execute(statement)
    results=cur.fetchall()
    # print(results)
    for i in results:
        region=i[0]
        if len(region)>12:
            region=region[:12]+'...'
        agg=i[1]
        if rating_counter>0:
            agg=str(round(agg,1))
        elif cocoa_counter>0:
            agg=str(round((agg*100),0))[:2]+'%'
        else:
            agg=str(agg)

        message=(
            f"{region.ljust(15)} "
            f"{agg.ljust(10)} "
        )
        print(message)

    conn.close()
    return results


def process_command(command):
    input=command.split()
    #print(input[0])
    output={}
    if 'bars' == input[0]:
        for i in input:
            if '=' in i:
                value=i.split('=')
                output[value[0]]=value[1]
            else:
                output[i]=''
        return bars_query(output)

    elif 'companies' == input[0]:
        #print('true')
        for i in input:
            if '=' in i:
                value=i.split('=')
                output[value[0]]=value[1]
            else:
                output[i]=''
        # print(output)
        return company_query(output)

    elif 'countries' == input[0]:
        #print('true')
        for i in input:
            if '=' in i:
                value=i.split('=')
                output[value[0]]=value[1]
            else:
                output[i]=''
        # print(output)
        return countries_query(output)
    elif 'regions' == input[0]:
        #print('true')
        for i in input:
            if '=' in i:
                value=i.split('=')
                output[value[0]]=value[1]
            else:
                output[i]=''
        # print(output)
        return regions_query(output)

#process_command('regions sources bars_sold top=5')

def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    drop_db()
    create_countries()
    populate_bars()

    while response != 'exit':
        response = input('Enter a command: ').lower()
        response_list=response.split()
        if response_list[0] == 'help':
            print(help_text)
            continue
        elif response_list[0] in ['bars', 'companies', 'countries', 'regions']:
            #print(response)
            process_command(response)
        elif response_list[0]=='exit':
            print('Bye!')
            exit()
        else:
            print('You have entered an invalid entry. Please type '+"'help'"+'to get instructions.')
            process_command(response)


#Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
