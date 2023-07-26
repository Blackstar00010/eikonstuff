

roa = atlagat * ni
cfroa = atlagat * (oancf + (oancf == 0) * (ib + dp))
xrdint = atlagat * xrd
capxint = atlagat * capx
xadint = atlagat * xad
ms = (roa > ser_to_df(roa.median(axis=1), roa.columns)) * 1 + \
     (cfroa > ser_to_df(cfroa.median(axis=1), cfroa.columns)) * 1 + \
     (xrdint > ser_to_df(xrdint.median(axis=1), xrdint.columns)) * 1 + \
     (capxint > ser_to_df(capxint.median(axis=1), capxint.columns)) * 1 + \
     (xadint > ser_to_df(xadint.median(axis=1), xadint.columns)) * 1 + \
     (oancf > ni) * 1