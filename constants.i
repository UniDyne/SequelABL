/* Required Apprise Includes */
&IF DEFINED(USE_API) &THEN
	{nonsdsuper.i}
	{APITables.i}
&ENDIF

/* Static Definitions */
&GLOBAL-DEFINE		carat		"^"


/* Apprise System ID */
&GLOBAL-DEFINE		sysid		"MyAppriseSysID"

/* Keys for typical queries */
&GLOBAL-DEFINE		lockey		"00000001"
&GLOBAL-DEFINE		mfglockey	"00000009"
&GLOBAL-DEFINE		cckey		"00000002"

&GLOBAL-DEFINE		promopbkey	"00000002"

&GLOBAL-DEFINE		dfltpickgrp	"1"
&GLOBAL-DEFINE		perspickgrp	"2"
