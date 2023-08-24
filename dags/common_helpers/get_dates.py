from datetime import datetime, timedelta
def get_dates(start=0, stop=-1):
    """
    :param start: How far ago do you want the pipeline to run. 0 means only today
    :param stop: when do you want the ETL to stop. Default is -1 meaning stop at the most recent file. I.e, when it's -1, last_modified will be tomorrow so it won't cut off any files
    :return: a date tuple with the start and stop dates
    """

    begin = datetime.today().date() - timedelta(days=start)
    end = datetime.today().date() - timedelta(days=stop)
    return begin, end

