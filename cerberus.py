import sys
from argparse import ArgumentParser
from src.network_status import readClients
from src.output import fileOut
from src.notify import slack


def main():
    # Supplies CLI arguments to run code
    parser = ArgumentParser()
    parser.add_argument('client_file', type=str, 
                        help = 'provide path to xlsx file with network info')
    parser.add_argument('ping_count', type=int, default=10, 
                        help = 'number of round-trip data packets to send')
    parser.add_argument('timeout', type=int, default=1,
                        help = 's (linux) or ms (windows) for ping timeout')
    parser.add_argument('--output_type', type=str, default='postgres',
                        help = 'output either to postgres or json files')
    args = parser.parse_args()

    # Supplies client information from excel file to the script
    rc = readClients(args.client_file, args.ping_count, args.timeout)
    f = fileOut(args.output_type)
    notify = slack()

    # The following function gathers client information from network pings
    network_status_dict = rc.clientInformation()

    # The following function writes the network ping metrics to a json output
    # or to a PostgreSQL database (comment out if you don't want one)
    if args.output_type == 'postgres':
        f.postgresAppend(network_status_dict)
    else:
        f.jsonOutput(network_status_dict)

    # Grab last week of network connection data from postgres
    # and store it as a pandas df
    connection_data = notify.queryData()
    notify.statusReport('src/temp/weeklyFileTime.pickle', 
                        connection_data)
    backhaul_status = notify.networkDisconnection(connection_data, 
                                                  'src/temp/backhaul.pickle')
    if backhaul_status == 'offline':
        sys.exit(0)
    notify.clientDisconnection(connection_data, 
                               'src/temp/client.pickle')

if __name__ == '__main__':
    main()
