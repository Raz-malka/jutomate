from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging



class Dates:

    today = datetime.today().replace(second=0, microsecond=0)


    @classmethod
    def get_datetime_obj(cls, date: str) -> datetime:
        if isinstance(date, str):
            try:
                date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S') 
            except:
                logging.exception(msg=f'date and time format needs to be "yyyy-mm-dd HH:MM:SS"!')
                return None
            return date

        logging.exception(msg=f'date needs to be a string!')


    @classmethod
    def get_next_day(cls, date: datetime)-> datetime:
        if date:
            if isinstance(date, datetime):
                max_day_in_month = date + relativedelta(day=35) # will give the full date of last day in the specified month

                if date.day < max_day_in_month.day:
                    date = date + relativedelta(day=date.day + 1)
                    
                elif date.month < 12:
                    date = date + relativedelta(day=1)
                    date = date + relativedelta(month=date.month + 1)

                else:
                    date = date + relativedelta(day=1)
                    date = date + relativedelta(month=1)
                    date = date + relativedelta(year=date.year + 1)

                return date

            logging.exception(msg=f'Date needs to be of type "datetime"! Consider using "get_datetime_obj" method')
        logging.exception(msg=f"Date can't be None!")


    @classmethod
    def set_time_to_begining_of_date(cls, date: datetime) -> datetime:
        if date:
            if isinstance(date, datetime):
                date = date + relativedelta(hour=00)
                date = date + relativedelta(minute=00)
                date = date + relativedelta(second=00)
                return date

            logging.exception(msg=f'Date needs to be of type "datetime"! Consider using "get_datetime_obj" method')
        logging.exception(msg=f"Date can't be None!")


    @classmethod
    def set_time_to_end_of_date(cls, date: datetime) -> datetime:
        if date:
            if isinstance(date, datetime):
                date = date + relativedelta(hour=23)
                date = date + relativedelta(minute=59)
                date = date + relativedelta(second=59)
                return date

            logging.exception(msg=f'Date needs to be of type "datetime"! Consider using "get_datetime_obj" method')
        logging.exception(msg=f"Date can't be None!")