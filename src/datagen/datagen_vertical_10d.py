import random

import sys
import optparse

parser = optparse.OptionParser()
parser.add_option('-t', '--num_of_trades', dest="num_of_trades", action="store", type="int")
parser.add_option('-s', '--num_of_scenarios', dest="num_of_scenarios",  action="store", type="int")

options, args = parser.parse_args()
num_of_trades = options.num_of_trades
num_of_scenarios = options.num_of_scenarios

print('\nError in Arguments provided. num_of_trades: {0} and num_of_scenarios: {1}\n'.format(num_of_trades, num_of_scenarios))
if (num_of_trades == None or num_of_scenarios == None):
	print("USAGE: \n"
		"       python {0} -t <num of trades> -s <num of scenarios>\n"
		"       OR\n"
		"       python {0} --num_of_trades <num of trades> --num_of_scenarios <num of scenarios>\n\n".format(sys.argv[0]) );


	print "Exiting ...\n"
	exit()

datafile = open('../../data/verticalData_10d.csv', 'w')

print("BDATE,DEAL_ID,PROD_ID,PORTFOLIO,SCENARIO,PNLS")

#num_of_trades = 1000
#num_of_scenarios = 100

bdate="20170228"
product_ids = ["PROD_001", "PROD_002", "PROD_003","PROD_004","PROD_005","PROD_006","PROD_007","PROD_008","PROD_009","PROD_010","PROD_011","PROD_012","PROD_013","PROD_014","PROD_015",]
portfolios = ["PORTF_001", "PORTF_002", "PORTF_003","PORTF_004","PORTF_005","PORTF_006","PORTF_007","PORTF_008","PORTF_009","PORTF_010"]

dimOne = ["dmOne_001", "dmOne_002", "dmOne_003","dmOne_004","dmOne_005","dmOne_006","dmOne_007","dmOne_008"]
dimTwo = ["dmTwo_001", "dmTwo_002", "dmTwo_003","dmTwo_004","dmTwo_005","dmTwo_006"]
dimThree = ["dmThree_001", "dmThree_002", "dmThree_003","dmThree_004","dmThree_005","dmThree_006","dmThree_007","dmThree_008","dmThree_009","dmThree_010"]
dimFour = ["dmFour_001", "dmFour_002", "dmFour_003","dmFour_004","dmFour_005"]
dimFive = ["dmFive_001", "dmFive_002", "dmFive_003","dmFive_004"]
dimSix = ["dmSix_001", "dmSix_002", "dmSix_003","dmSix_004","dmSix_005"]
dimSeven = ["dmSeven_001", "dmSeven_002", "dmSeven_003"]
dimEight = ["dmEight_001", "dmEight_002", "dmEight_003","dmEight_004"]

random.seed(200)

for line_num in range(1, num_of_trades+1):
    deal_id = "DEAL_" + str(line_num).rjust(8,'0')

    prod_index = random.randrange(0,15,1)
    product_id = product_ids[prod_index]

    portfolio_index = random.randrange(0,10,1)
    portfolio_id = portfolios[portfolio_index]

    dimOne_index = random.randrange(0,8,1)
    dimOne_id = dimOne[dimOne_index]

    dimTwo_index = random.randrange(0,6,1)
    dimTwo_id = dimTwo[dimTwo_index]

    dimThree_index = random.randrange(0,10,1)
    dimThree_id = dimThree[dimThree_index]

    dimFour_index = random.randrange(0,5,1)
    dimFour_id = dimFour[dimFour_index]

    dimFive_index = random.randrange(0,4,1)
    dimFive_id = dimFive[dimFive_index]

    dimSix_index = random.randrange(0,5,1)
    dimSix_id = dimSix[dimSix_index]

    dimSeven_index = random.randrange(0,3,1)
    dimSeven_id = dimSeven[dimSeven_index]

    dimEight_index = random.randrange(0,4,1)
    dimEight_id = dimEight[dimEight_index]

    pnl_str = ""

    # dealMu and dealSigma are meant to be distribution parameters for a deal.
    dealMu = random.uniform(-1,1)
    dealSigma = random.uniform(0.05,0.2)
    scenarioId = 0
    for xx in range(0, num_of_scenarios):
        pnl = format(random.normalvariate(dealMu, dealSigma) * 50, '.2f')
        dataline = bdate + "," + deal_id + "," + product_id + "," + portfolio_id + "," \
		   + dimOne_id + "," + dimTwo_id + "," + dimThree_id + "," \
		   + dimFour_id + "," + dimFive_id + "," + dimSix_id + "," \
		   + dimSeven_id + "," + dimEight_id + "," \
                   + str(scenarioId) + "," + str(pnl) + "\n"
        #print(dataline)
        datafile.write(dataline)

        scenarioId = scenarioId+1


datafile.close()


