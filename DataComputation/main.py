from annual_pt1 import *
from annual_pt2 import *
from annual_pt3 import *
from quarterly import *
from mohanram import *
# from ibes import *
import Misc.useful_stuff as us

if __name__ == '__main__':
    run_annual1(beep_on_finish=False)
    run_annual2(beep_on_finish=False)
    run_annual3(beep_on_finish=False)
    run_quarterly(beep_on_finish=False)
    run_mohanram(beep_on_finish=False)
    # run_ibes(beep_on_finish=False)
    us.beep()
