def upload_to_databricks(file_path, file_name, container_name):
    import os
    from timeit import default_timer as timer
    from azure.storage.blob import BlobClient, BlobServiceClient
    from pathlib import Path

    file_size = str(Path(file_path).stat().st_size)

    try:
        start = timer()
        blob_service_client = BlobServiceClient.from_connection_string(
            "DefaultEndpointsProtocol=https;AccountName=wdpstoragedev;AccountKey=mtPS4dX0lkHCgYCjcfdmLZEshY+5uWFhwqXhav1m7hUWWYzfD/fO0Zv6FhjIPIYcKKlr2vkIZeHPLRNc8+ztqQ==;EndpointSuffix=core.windows.net"
            # "BlobEndpoint=https://wdpstoragedev.blob.core.windows.net/;QueueEndpoint=https://wdpstoragedev.queue.core.windows.net/;FileEndpoint=https://wdpstoragedev.file.core.windows.net/;TableEndpoint=https://wdpstoragedev.table.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=o&sp=rwdlacupx&se=2020-08-07T17:36:46Z&st=2020-07-06T09:36:46Z&spr=https&sig=nKT4zbOgOKIt%2FRt8rplxCTq02XXLs23cjUQb78P%2B7kc%3D"
        )

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        print("\nUploading to Azure Storage as blob:\n\t" + file_name)
        # Upload the created file
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)
        end = timer()
        print('Uploaded {} bytes in {:.2f} seconds.'.format(file_size, end - start))

    except Exception as e:
        print('Could not upload file {}.\n'.format(file_path))
        print(e)

    return


def get_isec_data(query, lib_path, init=True):
    import sys
    import pandas as pd
    from timeit import default_timer as timer
    from sqlalchemy import create_engine
    import cx_Oracle

    start = timer()

    # Initialize Oracle client. Only needed in first function call
    try:
        print('Initializing Oracle client.')
        cx_Oracle.init_oracle_client(lib_dir=lib_path)
    except:
        print('Oracle client already started!')

    # Connection Info
    host = 'kwlwgunx002.grpitsrv.com'
    port = 1521
    sid = 'SPAN'
    user = 'readonly'
    password = 'readonly'

    # Start the connection
    print('Creating DSN.')
    sid = cx_Oracle.makedsn(host, port, sid=sid)

    cstr = 'oracle://{user}:{password}@{sid}'.format(
        user=user,
        password=password,
        sid=sid
    )

    print('Creating connection engine.')
    engine = create_engine(
        cstr,
        convert_unicode=False,
        pool_recycle=10,
        pool_size=50
    )

    print('Establishing connection.')
    try:
        conn = engine.connect()
        print('Connected.')
    except:
        sys.exit('ERROR: Connection error!')

    # Run the query and save the result in a pandas DF
    try:
        print('Running SQL query.')
        temp = pd.read_sql(sql=query, con=conn)

        # Close connection
        print('Closing connection.')
        conn.close()
    except:
        # Close connection
        print('ERROR: Query error! Closing connection.')
        conn.close()
        return pd.DataFrame({'A': []})

    end = timer()
    print('Extracted {} lines in {:.2f} seconds.\n'.format(temp.shape[0], end - start))
    return temp


def save_txt(isec_df, final_file_path, separator):
    from pathlib import Path
    from timeit import default_timer as timer

    start = timer()
    isec_df.to_csv(final_file_path, index=False, sep=separator)
    file_size = str(Path(final_file_path).stat().st_size)
    end = timer()
    print('Table saved in txt file with {} bytes in {:.2f} seconds'.format(file_size, end - start))

    return


def main():
    import datetime
    from timeit import default_timer as timer

    # Set paths for cx_Oracle lib, local csv and DBFS destination
    lib_path = r'C:\Users\menora\Desktop\transfer\instantclient_19_6'
    file_path = r"C:\Users\menora\Desktop\transfer"

    container_name = r'silver\andre'

    # If last_period = False then current period will be extracted. If true last complete period will be extracted
    last_period = False

    # Define list of tables to be extracted
    tables = ['ET0200']
    separator = "\ua746"

    # Calculate desired period for the ET0*00 tables
    dt = datetime.datetime.now()
    timestamp = dt
    if last_period:
        dt = dt - datetime.timedelta(days=dt.day)
    dt = dt.strftime("%Y%m")
    timestamp = timestamp.strftime("%Y%m%d%H%M%S")

    # Run the extraction on selected tables
    for tb in tables:

        # Set complete path for each table
        file_name = 'ISEC_{table}_{date}.txt'.format(table=tb, date=timestamp)
        final_file_path = file_path + '\\' + file_name  # trocar para path com barra de unix /

        # Run query
        print('\nExtracting table {}.'.format(tb))
        print('Query:\n SELECT OPSSOM1.{table}_{date}.* FROM OPSSOM1.{table}_{date}\n'.format(table=tb, date=dt))
        print('Started at ', datetime.datetime.now())
        isec_df = get_isec_data(
            r"SELECT OPSSOM1.{table}_{date}.* FROM OPSSOM1.{table}_{date}".format(table=tb, date=dt), lib_path)

        # Save query result into csv
        if isec_df.empty:
            print('Dataframe empty. Failed to extract table {} '.format(tb))
        else:
            print('Saving table {} to {}'.format(tb, final_file_path))
            print('Started at ', datetime.datetime.now())
            save_txt(isec_df, final_file_path, separator)

            # Upload it into DBFS
            print('Uploading {}\n'.format(final_file_path))
            print('Started at ', datetime.datetime.now())
            upload_to_databricks(final_file_path, file_name, container_name)

    print('Done!')


if __name__ == "__main__":
    main()
