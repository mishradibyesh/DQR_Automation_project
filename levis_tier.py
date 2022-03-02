"""
@author: Dibyesh Mishra
@date: 24-02-2022 22:48
"""
import re

import pymysql
pymysql.install_as_MySQLdb()


class Functionality:
    """
    class Functionality have methods which shows data from table
    in database programmigration
    """
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='root',
                                 db='programmigration')
    cursor = connection.cursor()

    def total_number_of_records(self, table_name):
        """
        desc: calculating the total rows in the table
        return: data_list
        """
        query = "select count(mobile) from %s " % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(result[0][0])
        self.connection.commit()
        query3 = "insert into tier_dqr(Total_number_of_rows) values(%d)" % result[0][0]
        self.cursor.execute(query3)
        self.connection.commit()

    def total_rows_with_blank_mobile_no(self, table_name):
        """
        desc: counting the total number of blank mobile numbers
        return: count
        """
        query = "select mobile from %s " % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        i = 0
        count = 0
        while i < len(result):
            if not result[i][0]:
                count = count+1
            i = i+1
        print(count)
        self.connection.commit()
        return count

    def total_rows_with_invalid_mobile_no(self, table_name):
        """
        desc: counting the total number of invalid mobile numbers and inserting those into tier_error table
        return: count
        """
        query = "select mobile from %s " % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(result)
        i = 0
        count = 0
        while i < len(result):
            r = re.match('^[8-9]{1}[0-9]{9}', result[i][0])
            if not r:
                count = count + 1
                query2 ="INSERT INTO tier_error(Mobile,FirstName,LastName,MembershipCardNo,PreviousTier,CurrentTier," \
                        "TierStartDate,TierEndDate,TierChangeDate ,TierChangeType,DateOfEnrollment,EnrolledStore," \
                        "LifetimeSpends,LifetimeBills ,LastTxnDate ,RowNumber,RecordCount,LpassLastUpdated) " \
                        "SELECT * FROM levis_tier WHERE Mobile = %s;  " % result[i][0]
                self.cursor.execute(query2)
                self.connection.commit()
                query3 = "UPDATE tier_error SET reason ='Mobile number is Invalid' WHERE Mobile = %s" % result[i][0]
                self.cursor.execute(query3)
                self.connection.commit()
            i = i + 1
        return count

    def total_rows_with_duplicate_mobile_no(self, table_name):
        """
        desc: finding the total number of duplicate mobile number
        """
        query = "SELECT COUNT(*) AS Total_duplicate_count FROM (SELECT Mobile FROM %s GROUP BY Mobile HAVING " \
                "COUNT(Mobile) > 1) AS X" % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(result[0][0])
        self.connection.commit()
        return result[0][0]

    def total_count_if_current_tier_is_null(self, table_name):
        """
        desc: calculating the total count if current tier is null
        return: count
        """
        query = "select CurrentTier from %s " % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        i = 0
        count = 0
        while i < len(result):
            if not result[i][0]:
                count = count + 1
            i = i + 1
        print(count)
        self.connection.commit()
        return count

    def total_count_if_previous_tier_is_null(self, table_name):
        """
        desc: calculating the total count if previous tier is null
        return: count
        """
        query = "select PreviousTier from %s " % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        i = 0
        count = 0
        while i < len(result):
            if not result[i][0]:
                count = count + 1
            i = i + 1
        print(count)
        self.connection.commit()
        return count

    def total_count_of_first_names_having_null_value(self, table_name):
        """
        desc: calculating the total count of first names having null value in the table
        """
        query = "SELECT SUM(CASE WHEN FirstName IS NULL THEN 1 ELSE 0 END) AS NUMBER_OF_NULL_VALUE FROM %s" % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(result[0][0])
        self.connection.commit()
        query3 = "insert into tier_dqr(total_count_of_names_having_null_value) values(%d)" % result[0][0]
        self.cursor.execute(query3)
        self.connection.commit()

    def total_count_of_last_name_having_null_value(self, table_name):
        """
        desc: calculating the total count of last_names having null values in the table
        return: total count
        """
        query = "SELECT SUM(CASE WHEN LastName IS NULL THEN 1 ELSE 0 END) AS NUMBER_OF_NULL_VALUE FROM %s" % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(result[0][0])
        self.connection.commit()
        query3 = "insert into tier_dqr(total_count_of_last_name_having_null_value) values(%d)" % result[0][0]
        self.cursor.execute(query3)
        self.connection.commit()

    def total_sales_in_current_tier(self,table_name):
        """
        desc: calculating total sales in current Tier
        return: total sales
        """
        query = "SELECT SUM(LifetimeSpends) FROM levis_tier"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(result[0][0])
        return result[0][0]

    def insert_into_dqr(self,table_name):
        """
        desc: inserting the values into the tier_dqr table
        """
        total_rows_with_blank_mobile = self.total_rows_with_blank_mobile_no("levis_tier")
        total_rows_with_invalid_mobile = self.total_rows_with_invalid_mobile_no("levis_tier")
        count_if_current_tier_is_null = self.total_count_if_current_tier_is_null("levis_tier")
        count_if_previous_tier_is_null = self.total_count_if_previous_tier_is_null("levis_tier")
        total_sales_in_current_tier = self.total_sales_in_current_tier("levis_tier")

        query3 = "insert into %s(Total_Rows_with_Blank_Mobile,Total_Rows_with_Invalid_Mobile," \
                 "Current_tier_is_null,Previous_tier,Total_Sales_in_current_tier) values(%s,%s,%s,%s,%s)"\
                                                                            % (table_name,total_rows_with_blank_mobile,
                                                                                total_rows_with_invalid_mobile,
                                                                                count_if_current_tier_is_null,
                                                                                count_if_previous_tier_is_null,
                                                                                total_sales_in_current_tier)
        self.cursor.execute(query3)
        self.connection.commit()



data = Functionality()
# data.total_number_of_rows("levis_tier")
# data.total_rows_with_blank_mobile_no("levis_tier")
data.total_rows_with_invalid_mobile_no("levis_tier")
# data.total_rows_with_duplicate_mobile_no("levis_tier")
# data.current_tier_is_null("levis_tier")
# data.insert_into_dqr("tier_dqr")
# data.total_count_of_names_having_null_value("levis_tier")
# data.total_count_of_last_name_having_null_value("levis_tier")
# data.previous_tier_is_null("levis_tier")
# data.total_sales_in_current_tier("levis_tier")

