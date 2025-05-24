from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime, timedelta
import time
import warnings


class PytrendsExtractor:

    def fetch_trends_in_loops(self, keyword='Bitcoin', start_date='2023-01-01',
                            batch_len=30, wait_time=10, retry_wait_time=60, drop_partial=True, **kwargs):
        """
        Fetch Google Trends data for a single keyword in loops over time.
        """
        from pytrends.exceptions import ResponseError

        pytrends = TrendReq(hl='en-US', tz=360)
        df = pd.DataFrame()

        # Convert start_date string to a datetime object
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = start_date + timedelta(days=batch_len)

        today = datetime.today()

        while start_date < today:
            if end_date > today:
                end_date = today

            timeframe = f'{start_date.strftime("%Y-%m-%d")} {end_date.strftime("%Y-%m-%d")}'
            print(f"Fetching timeframe: {timeframe}")

            while True:
                try:
                    # Pass additional kwargs to build_payload
                    pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo='', gprop='', **kwargs)

                    # Suppress the warning temporarily during data fetching
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", FutureWarning)
                        interest_over_time_df = pytrends.interest_over_time()

                    # Proactively handle NaN values to avoid issues
                    interest_over_time_df = interest_over_time_df.infer_objects(copy=False)
                    interest_over_time_df.fillna(False, inplace=True)
                    break  # Exit retry loop once successful
                except ResponseError as e:
                    print(f"Error: {e}. Retrying in {retry_wait_time} seconds...")
                    time.sleep(retry_wait_time)
                except Exception as e:
                    print(f"Unexpected error: {e}")
                    return None

            # Append the data to the df DataFrame
            df = pd.concat([df, interest_over_time_df])

            # Wait for the specified time before continuing to the next loop
            time.sleep(wait_time)

            # Move the window forward
            start_date = end_date + timedelta(days=1)
            end_date = start_date + timedelta(days=batch_len)

        # Clean data frame
        if drop_partial:
            df = df[df['isPartial'] != True]
            df = df.drop(columns=['isPartial'])
        df.columns = [col.capitalize() for col in df.columns]

        # Reset index
        df.reset_index(inplace=True)
        self.df = df
        return df



    def fetch_trends_for_keywords(self, keywords, start_date='2023-01-01', 
                                batch_len=30, wait_time=10, 
                                retry_wait_time=60, drop_partial=True, **kwargs):
        """
        Fetch Google Trends data for a list of keywords and combine results into a single DataFrame.

        Args:
            keywords (list): List of keywords to fetch trends for.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            batch_len (int): Number of days per batch.
            wait_time (int): Wait time between batches in seconds.
            retry_wait_time (int): Wait time between retries in seconds.
            drop_partial (bool): Whether to drop partial data.

        Returns:
            pd.DataFrame: Combined trends data for all keywords.
        """
        combined_df = pd.DataFrame()

        for keyword in keywords:
            print(f"Fetching trends for keyword: {keyword}")
            # Call the existing function for each keyword, passing **kwargs
            keyword_df = self.fetch_trends_in_loops(keyword=keyword, 
                                                    start_date=start_date, 
                                                    batch_len=batch_len, 
                                                    wait_time=wait_time, 
                                                    retry_wait_time=retry_wait_time, 
                                                    drop_partial=drop_partial,
                                                    **kwargs)

            if keyword_df is not None and not keyword_df.empty:
                # Rename the 'Interest' column to include the keyword
                keyword_df.rename(columns={'Interest': keyword}, inplace=True)
                # Drop the original index if it exists
                if 'index' in keyword_df.columns:
                    keyword_df.drop(columns=['index'], inplace=True)
                # Combine the results
                if combined_df.empty:
                    combined_df = keyword_df
                else:
                    combined_df = pd.merge(combined_df, keyword_df, on='date', how='outer')

        # Reset index of the combined DataFrame
        combined_df.reset_index(drop=True, inplace=True)
        self.df = combined_df
        self.df_google_trends = combined_df
