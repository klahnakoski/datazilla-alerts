DELIMITER ;;

DROP DATABASE IF EXISTS json;;
CREATE DATABASE json;;
USE json;;


-- JSON GET OBJECT
-- RETURN THE JSON OBJECT REFERENCED BY TAG NAME
-- FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS json;;
CREATE FUNCTION json (
	value		longtext character set utf8,
	tag			VARCHAR(40)
) RETURNS
	varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s DECIMAL(10,0);
	DECLARE i DECIMAL(10,0);
	DECLARE c CHAR;
	DECLARE d INTEGER; # DEPTH
	DECLARE begin_tag VARCHAR(50);

	IF value IS NOT NULL THEN
		SET begin_tag=concat("\"", tag, "\":");
		SET s=locate(begin_tag, value);
		IF s=0 THEN
			RETURN NULL;
		ELSE
			SET s=locate("{", value, s+length(begin_tag));
			SET i=s+1;
			SET d=1;
			DD: LOOP
				SET c=substring(value, i, 1);
				IF c="\"" THEN
					SET i=i+1;
					QQ: LOOP
						SET c=substring(value, i, 1);
						IF c="\\" THEN
							SET i=i+1;
						ELSEIF c="\"" THEN
							LEAVE QQ;
						END IF;
						SET i=i+1;
						IF i>length(value) OR i-s>65000 THEN LEAVE DD; END IF;
					END LOOP QQ;
				ELSEIF c="{" OR c="[" THEN
					SET d=d+1;
				ELSEIF c="}" OR c="]" THEN
					SET d=d-1;
				END IF;
				SET i=i+1;
				IF d=0 OR i>length(value) OR i-s>65000 THEN LEAVE DD; END IF;
			END LOOP DD;
			RETURN substring(value, s, i-s);
		END IF;
	ELSE
		RETURN NULL;
	END IF;
END;;

SELECT json(" [\"results\": {}, junk]", "results") result, "{}" expected from dual;;
SELECT json(" [\"results\": {\"hi\":20}, junk]", "results") result, "{\"hi\":20}" expected from dual;;
SELECT json(" \"results\": {\"hi\":20}, junk]", "results") result, "{\"hi\":20}" expected  from dual;;
SELECT json(" [\"results\": {\"some thing\":[324,987]}, junk]", "results") result, "{\"some thing\":[324,987]}" expected  from dual;;
SELECT json(" \"results\": {\"some thing\":[324,987], {\"other\":\"99\\\"\"}}, jumk", "results") result, "{\"some thing\":[324,987], {\"other\":\"99\\\"\"}}" expected  from dual;;
SELECT json(" \"results\": {\"some thing\":[324,987], {\"other\":\"99\\\"}}, jumk", "results") result, "{\"some thing\":[324,987], {\"other\":\"99\\\"}}, jumk" expected  from dual;;
SELECT json('{"example": {"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "linux", "version": "Ubuntu 12.04"}, "Platform": "x86_64", "Product": "Firefox", "Revision": "62a76f959ae7", "Test": {"name": "sessionrestore_no_auto_restore", "suite": "sessionrestore_no_auto_restore"}}, "datazilla": {"url": {"branch": "Mozilla-Inbound", "stop": 1399524351000, "x86": "false", "x86_64": "true"}}, "diff": -64.5, "diff_percent": -0.05081908013525492, "future_is_diff": false, "future_stats": {"count": 12, "kurtosis": -1.7395279272587136, "mean": 1204.7083333333333, "samples": [1199.5, 1205.0, 1217.5, 1197.5, 1218.0, 1215.5, 1193.0, 1178.5, 1215.5, 1190.0, 1189.5, 1217.5, 1221.5, 1226.5, 1196.5, 1216.5, 1192.0, 1188.0, 1194.0, 1214.0], "skew": 0.09330496385618409, "variance": 98.47743055573665}, "ignored": false, "is_diff": true, "mercurial": {"url": {"branch": "integration/mozilla-inbound"}}, "pass": true, "past_is_diff": false, "past_revision": "997a2d66710e", "past_stats": {"count": 12, "kurtosis": -1.1467143561378705, "mean": 1269.2083333333333, "samples": [1284.0, 1271.5, 1278.0, 1281.0, 1262.5, 1265.0, 1269.5, 1264.5, 1267.0, 1272.5, 1277.5, 1274.0, 1257.0, 1260.5, 1312.5, 1260.5, 1268.5, 1274.0, 1260.0, 1264.0], "skew": 0.18124515059142324, "variance": 20.6857638892252}, "push_date": 1399351958000, "push_date_max": 1399524351000, "push_date_min": 1399102886000, "result": {"confidence": 0.9999999895988725, "diff": 40.0}, "tbpl": {"url": {"branch": "Mozilla-Inbound"}}, "test_run_id": 5402677, "value": 1199.5}, "revision": "62a76f959ae7", "tests": [{"confidence": 0.999999989599, "example": {"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "linux", "version": "Ubuntu 12.04"}, "Platform": "x86_64", "Product": "Firefox", "Revision": "62a76f959ae7", "Test": {"name": "sessionrestore_no_auto_restore", "suite": "sessionrestore_no_auto_restore"}}, "datazilla": {"url": {"branch": "Mozilla-Inbound", "stop": 1399524351000, "x86": "false", "x86_64": "true"}}, "diff": -64.5, "diff_percent": -0.05081908013525492, "future_is_diff": false, "future_stats": {"count": 12, "kurtosis": -1.7395279272587136, "mean": 1204.7083333333333, "samples": [1199.5, 1205.0, 1217.5, 1197.5, 1218.0, 1215.5, 1193.0, 1178.5, 1215.5, 1190.0, 1189.5, 1217.5, 1221.5, 1226.5, 1196.5, 1216.5, 1192.0, 1188.0, 1194.0, 1214.0], "skew": 0.09330496385618409, "variance": 98.47743055573665}, "ignored": false, "is_diff": true, "mercurial": {"url": {"branch": "integration/mozilla-inbound"}}, "pass": true, "past_is_diff": false, "past_revision": "997a2d66710e", "past_stats": {"count": 12, "kurtosis": -1.1467143561378705, "mean": 1269.2083333333333, "samples": [1284.0, 1271.5, 1278.0, 1281.0, 1262.5, 1265.0, 1269.5, 1264.5, 1267.0, 1272.5, 1277.5, 1274.0, 1257.0, 1260.5, 1312.5, 1260.5, 1268.5, 1274.0, 1260.0, 1264.0], "skew": 0.18124515059142324, "variance": 20.6857638892252}, "push_date": 1399351958000, "push_date_max": 1399524351000, "push_date_min": 1399102886000, "result": {"confidence": 0.9999999895988725, "diff": 40.0}, "tbpl": {"url": {"branch": "Mozilla-Inbound"}}, "test_run_id": 5402677, "value": 1199.5}, "num_exceptions": 3, "num_tests": [], "test": {"name": "sessionrestore_no_auto_restore", "suite": "sessionrestore_no_auto_restore"}}, {"confidence": 0.999999989599, "example": {"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "win", "version": "6.2.9200"}, "Platform": "x86_64", "Product": "Firefox", "Revision": "62a76f959ae7", "Test": {"name": "sessionrestore", "suite": "sessionrestore"}}, "datazilla": {"url": {"branch": "Mozilla-Inbound", "stop": 1399514844000, "x86": "false", "x86_64": "true"}}, "diff": -70.20833333333326, "diff_percent": -0.04891004615250641, "future_is_diff": false, "future_stats": {"count": 12, "kurtosis": 0.2006920940493342, "mean": 1365.25, "samples": [1338.0, 1302.5, 1377.0, 1370.0, 1376.0, 1375.0, 1371.0, 1360.0, 1365.0, 1373.5, 1380.5, 1271.5, 1361.5, 1344.0, 1297.0, 1374.0, 1372.0, 1355.5, 1382.5, 1361.5], "skew": -0.9577729912837829, "variance": 78.60416666674428}, "ignored": false, "is_diff": true, "mercurial": {"url": {"branch": "integration/mozilla-inbound"}}, "pass": true, "past_is_diff": false, "past_revision": "997a2d66710e", "past_stats": {"count": 12, "kurtosis": -0.45574625215313125, "mean": 1435.4583333333333, "samples": [1430.5, 1440.0, 1436.5, 1455.0, 1449.0, 1416.0, 1438.0, 1454.5, 1436.5, 1427.5, 1473.0, 1440.5, 1428.8333333333333, 1433.5, 1435.0, 1416.0, 1425.0, 1446.1666666666667, 1432.5, 1414.5], "skew": 0.3226985676047139, "variance": 26.162615740904585}, "push_date": 1399356008000, "push_date_max": 1399514844000, "push_date_min": 1399107033000, "result": {"confidence": 0.9999999895988725, "diff": 40.0}, "tbpl": {"url": {"branch": "Mozilla-Inbound"}}, "test_run_id": 5403614, "value": 1338.0}, "num_exceptions": 3, "num_tests": [], "test": {"name": "sessionrestore", "suite": "sessionrestore"}}, {"confidence": 0.999830257759, "example": {"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "win", "version": "6.1.7601"}, "Platform": "x86", "Product": "Firefox", "Revision": "62a76f959ae7", "Test": {"name": "newtab-open-preload-yes.error.TART", "suite": "tart"}}, "datazilla": {"url": {"branch": "Mozilla-Inbound", "stop": 1399442365000, "x86": "true", "x86_64": "false"}}, "diff": -1.9185260993035662, "diff_percent": -0.059158105294335336, "future_is_diff": false, "future_stats": {"count": 6, "kurtosis": -1.5874141996086844, "mean": 30.511959795370178, "samples": [30.80266705781105, 30.450758374288853, 30.80957892157312, 30.175585659890203, 30.261984587123152, 30.158920877060154, 30.702678127097897, 30.696283194945863, 30.784468828875106, 30.06601623215829], "skew": -0.2719330769919991, "variance": 0.0539984247884604}, "ignored": false, "is_diff": true, "mercurial": {"url": {"branch": "integration/mozilla-inbound"}}, "pass": true, "past_is_diff": false, "past_revision": "997a2d66710e", "past_stats": {"count": 6, "kurtosis": -0.15870823514150656, "mean": 32.430485894673744, "samples": [33.020990331380744, 32.84128287262865, 31.848890383917023, 32.45022922681528, 32.4572853303398, 32.41811781665092, 32.10592969277059, 32.97961852856679, 32.05140236974694, 32.31007042883721], "skew": 0.5245101084436394, "variance": 0.04830862466224062}, "push_date": 1399356224000, "push_date_max": 1399442365000, "push_date_min": 1399258656000, "result": {"confidence": 0.9998302577586308, "diff": 20.0}, "tbpl": {"url": {"branch": "Mozilla-Inbound"}}, "test_run_id": 5403648, "value": 30.80266705781105}, "num_exceptions": 2, "num_tests": [], "test": {"name": "newtab-open-preload-yes.error.TART", "suite": "tart"}}, {"confidence": 0.999830257759, "example": {"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "linux", "version": "Ubuntu 12.04"}, "Platform": "x86", "Product": "Firefox", "Revision": "62a76f959ae7", "Test": {"name": "ai-astar", "suite": "kraken"}}, "datazilla": {"url": {"branch": "Mozilla-Inbound", "stop": 1399439195000, "x86": "true", "x86_64": "false"}}, "diff": -3.4138888888888914, "diff_percent": -0.029670223552701484, "future_is_diff": false, "future_stats": {"count": 6, "kurtosis": -1.730079787560515, "mean": 111.64722222222223, "samples": [111.25, 111.3, 111.5, 112.83333333333333, 112.0, 112.25, 111.83333333333333, 111.1, 112.0, 111.0], "skew": -0.07403960613249398, "variance": 0.09726080246764468}, "ignored": false, "is_diff": true, "mercurial": {"url": {"branch": "integration/mozilla-inbound"}}, "pass": true, "past_is_diff": false, "past_revision": "997a2d66710e", "past_stats": {"count": 6, "kurtosis": -1.6598199609740467, "mean": 115.06111111111112, "samples": [115.5, 114.9, 115.5, 114.66666666666667, 115.3, 115.66666666666667, 115.3, 114.16666666666667, 114.66666666666667, 114.7], "skew": -0.004204746009875475, "variance": 0.10311728394844977}, "push_date": 1399352650000, "push_date_max": 1399439195000, "push_date_min": 1399257063000, "result": {"confidence": 0.9998302577586308, "diff": 20.0}, "tbpl": {"url": {"branch": "Mozilla-Inbound"}}, "test_run_id": 5402808, "value": 111.25}, "num_exceptions": 1, "num_tests": [], "test": {"name": "ai-astar", "suite": "kraken"}}, {"confidence": 0.999830257759, "example": {"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "win", "version": "5.1.2600"}, "Platform": "x86", "Product": "Firefox", "Revision": "62a76f959ae7", "Test": {"name": "icon-open-DPI2.error.TART", "suite": "tart"}}, "datazilla": {"url": {"branch": "Mozilla-Inbound", "stop": 1399442456000, "x86": "true", "x86_64": "false"}}, "diff": -1.8407367416751477, "diff_percent": -0.07331308418038776, "future_is_diff": false, "future_stats": {"count": 6, "kurtosis": -1.6947896278498544, "mean": 23.267151737630837, "samples": [23.345886139170034, 23.408463924890384, 23.38611471571494, 23.14362579601584, 23.572730612417217, 23.116806744947098, 23.382762334318613, 22.84889559983276, 23.227714695618488, 22.89554957403743], "skew": -0.20157797793229879, "variance": 0.012189110260919733}, "ignored": false, "is_diff": true, "mercurial": {"url": {"branch": "integration/mozilla-inbound"}}, "pass": true, "past_is_diff": false, "past_revision": "997a2d66710e", "past_stats": {"count": 6, "kurtosis": -0.4984763581275171, "mean": 25.107888479305984, "samples": [25.178241927394993, 25.09135937668907, 25.510127683810424, 24.841048233764013, 25.074597469792934, 29.631042492837878, 25.170140339068894, 25.182432404108113, 24.706673613516614, 24.950559358781902], "skew": -0.8806546184821712, "variance": 0.006752608856686493}, "push_date": 1399356189000, "push_date_max": 1399442456000, "push_date_min": 1399258735000, "result": {"confidence": 0.9998302577586308, "diff": 20.0}, "tbpl": {"url": {"branch": "Mozilla-Inbound"}}, "test_run_id": 5403646, "value": 23.345886139170034}, "num_exceptions": 2, "num_tests": [], "test": {"name": "icon-open-DPI2.error.TART", "suite": "tart"}}, {"confidence": 0.999830257759, "example": {"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "win", "version": "5.1.2600"}, "Platform": "x86", "Product": "Firefox", "Revision": "62a76f959ae7", "Test": {"name": "simple-open-DPI1.error.TART", "suite": "tart"}}, "datazilla": {"url": {"branch": "Mozilla-Inbound", "stop": 1399442456000, "x86": "true", "x86_64": "false"}}, "diff": -1.5964319487526772, "diff_percent": -0.07127520185490396, "future_is_diff": false, "future_stats": {"count": 6, "kurtosis": -0.5260965211818003, "mean": 20.801708038315457, "samples": [21.087219185661525, 20.737733427005878, 21.148958876059623, 20.550838165188907, 21.04419695798424, 20.69526992959436, 20.78215248027118, 20.612577855528798, 20.879371540242573, 20.671523894794518], "skew": 0.8875410925863673, "variance": 0.01626410810820289}, "ignored": false, "is_diff": true, "mercurial": {"url": {"branch": "integration/mozilla-inbound"}}, "pass": true, "past_is_diff": false, "past_revision": "997a2d66710e", "past_stats": {"count": 6, "kurtosis": -1.8391955801549262, "mean": 22.398139987068134, "samples": [22.260552667998127, 22.50974635046441, 22.48320666453219, 22.206914565962506, 22.294914577134477, 23.877517952714697, 22.838838455674704, 22.511981271360128, 22.328438390919473, 22.213619328686036], "skew": -0.07920884272020873, "variance": 0.011182704350801487}, "push_date": 1399356189000, "push_date_max": 1399442456000, "push_date_min": 1399258735000, "result": {"confidence": 0.9998302577586308, "diff": 20.0}, "tbpl": {"url": {"branch": "Mozilla-Inbound"}}, "test_run_id": 5403646, "value": 21.087219185661525}, "num_exceptions": 2, "num_tests": [], "test": {"name": "simple-open-DPI1.error.TART", "suite": "tart"}}, {"confidence": 0.999830257759, "example": {"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "linux", "version": "Ubuntu 12.04"}, "Platform": "x86", "Product": "Firefox", "Revision": "62a76f959ae7", "Test": {"name": "audio-oscillator", "suite": "kraken"}}, "datazilla": {"url": {"branch": "Mozilla-Inbound", "stop": 1399439195000, "x86": "true", "x86_64": "false"}}, "diff": -6.677777777777777, "diff_percent": -0.06339662447257384, "future_is_diff": false, "future_stats": {"count": 6, "kurtosis": -1.6714750155664166, "mean": 98.65555555555555, "samples": [99.5, 98.83333333333333, 99.5, 98.1, 99.1, 98.5, 98.25, 99.0, 98.25, 98.0], "skew": -0.012088708848201334, "variance": 0.11682098765595583}, "ignored": false, "is_diff": true, "mercurial": {"url": {"branch": "integration/mozilla-inbound"}}, "pass": true, "past_is_diff": false, "past_revision": "997a2d66710e", "past_stats": {"count": 6, "kurtosis": 0.7974363546286707, "mean": 105.33333333333333, "samples": [102.5, 104.5, 110.5, 105.0, 105.0, 108.5, 102.5, 109.5, 104.0, 105.0], "skew": 1.528847733395095, "variance": 2.1388888888905058}, "push_date": 1399352650000, "push_date_max": 1399439195000, "push_date_min": 1399257063000, "result": {"confidence": 0.9998302577586308, "diff": 20.0}, "tbpl": {"url": {"branch": "Mozilla-Inbound"}}, "test_run_id": 5402808, "value": 99.5}, "num_exceptions": 2, "num_tests": [], "test": {"name": "audio-oscillator", "suite": "kraken"}}, {"confidence": 0.999830257759, "example": {"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "win", "version": "5.1.2600"}, "Platform": "x86", "Product": "Firefox", "Revision": "62a76f959ae7", "Test": {"name": "newtab-open-preload-no.error.TART", "suite": "tart"}}, "datazilla": {"url": {"branch": "Mozilla-Inbound", "stop": 1399442456000, "x86": "true", "x86_64": "false"}}, "diff": -1.8741208728946113, "diff_percent": -0.0802103711322413, "future_is_diff": false, "future_stats": {"count": 6, "kurtosis": -0.6857567651927474, "mean": 21.490948337479495, "samples": [21.727244663765305, 21.46464145583741, 21.73786053812364, 21.20120015257271, 21.758812921762, 21.492857332414133, 21.635892271224293, 21.090850932167086, 21.423854149063118, 21.198127136274707], "skew": -0.30448191095371574, "variance": 0.027666005800767834}, "ignored": false, "is_diff": true, "mercurial": {"url": {"branch": "integration/mozilla-inbound"}}, "pass": true, "past_is_diff": false, "past_revision": "997a2d66710e", "past_stats": {"count": 6, "kurtosis": -0.07983253873991147, "mean": 23.365069210374106, "samples": [23.759625874241465, 23.44980996187951, 23.28777819527022, 23.00897181063192, 23.25313692103373, 24.789645052689593, 23.28274962317664, 23.7207941232773, 22.823752739524934, 23.19614643760724], "skew": 1.1575893431799276, "variance": 0.031255894105697735}, "push_date": 1399356189000, "push_date_max": 1399442456000, "push_date_min": 1399258735000, "result": {"confidence": 0.9998302577586308, "diff": 20.0}, "tbpl": {"url": {"branch": "Mozilla-Inbound"}}, "test_run_id": 5403646, "value": 21.727244663765305}, "num_exceptions": 2, "num_tests": [], "test": {"name": "newtab-open-preload-no.error.TART", "suite": "tart"}}, {"confidence": 0.998107092512, "example": {"Talos": {"Branch": "Mozilla-Inbound", "OS": {"name": "win", "version": "6.2.9200"}, "Platform": "x86_64", "Product": "Firefox", "Revision": "62a76f959ae7", "Test": {"name": "tpaint", "suite": "tpaint"}}, "datazilla": {"url": {"branch": "Mozilla-Inbound", "stop": 1399441865000, "x86": "false", "x86_64": "true"}}, "diff": -2.657741938695267, "diff_percent": -0.017608920610136587, "future_is_diff": false, "future_stats": {"count": 6, "kurtosis": -1.3110533104315099, "mean": 148.27382266642516, "samples": [148.87904826526392, 147.36623831439283, 148.46826200787427, 149.54461428342165, 147.57890508810215, 147.26491879927653, 148.91437620647775, 148.4792269626114, 148.50785585591166, 147.72963781878752], "skew": -0.4163934529553682, "variance": 0.2133524551136361}, "ignored": false, "is_diff": true, "mercurial": {"url": {"branch": "integration/mozilla-inbound"}}, "pass": true, "past_is_diff": false, "past_revision": "997a2d66710e", "past_stats": {"count": 6, "kurtosis": -1.2000206570942449, "mean": 150.93156460512043, "samples": [152.17262748188205, 150.7799425996667, 151.24195279484957, 151.0207106613011, 151.86053593295856, 149.2143943039024, 150.5314553197377, 150.43478427826176, 150.04454394298318, 151.5805419769058], "skew": 0.2985403855945813, "variance": 0.1592211404495174}, "push_date": 1399356008000, "push_date_max": 1399441865000, "push_date_min": 1399258139000, "result": {"confidence": 0.9981070925124622, "diff": 14.912570815624479}, "tbpl": {"url": {"branch": "Mozilla-Inbound"}}, "test_run_id": 5403618, "value": 148.87904826526392}, "num_exceptions": 1, "num_tests": [], "test": {"name": "tpaint", "suite": "tpaint"}}], "total_exceptions": 18, "total_tests": []}',
"datazilla") result, '{"url": {"branch": "Mozilla-Inbound", "stop": 1399524351000, "x86": "false", "x86_64": "true"}}' expected from dual;;
SELECT json(NULL, "datzilla") result, NULL expected from dual;;

-- JSON GET STRING
-- RETURN STRING REFERENCED BY TAG VALUE
-- FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS string;;
CREATE FUNCTION string (
	value		longtext character set utf8,
	tag			VARCHAR(40)
) RETURNS varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);

	SET begin_tag=concat("\"", tag, "\":");
	IF instr(value, begin_tag)=0 THEN
		RETURN NULL;
	ELSE
		RETURN string.between(substring(value, instr(value, begin_tag)+length(begin_tag), 65000), "\"", "\"", 1);
	END IF;
END;;

-- JSON GET NUMBER
-- RETURN A NUMERIC VALUE REFERNCED BY TAG
-- FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS number;;
CREATE FUNCTION number (
	value		longtext character set utf8,
	tag			VARCHAR(40)
) RETURNS varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);
	DECLARE begin_data INTEGER;
	DECLARE end_data INTEGER;

	SET begin_tag=concat("\"", tag, "\":");
	SET begin_data=instr(value, begin_tag);
	IF begin_data=0 THEN
		RETURN NULL;
	ELSE
		SET begin_data=begin_data+length(begin_tag)-1;
		SET end_data=math.minof(string.locate(',', value, begin_data), string.locate('}', value, begin_data));
		RETURN trim(substring(value, begin_data+1, end_data-begin_data-1));
	END IF;
END;;

SELECT number(" \"results\": {\"last number\":324}, jumk", "last number") result, 324 expected from dual;;
SELECT number('{"count": 20, "skew": 1.4737744878964223, "unbiased": true, "variance": 5.211875000037253, "kurtosis": 2.6389621539338295, "mean": 715.775}', "mean") result, "715.775" expected from dual;;


-- JSON GET ARRAY
-- RETURN ARRAY REFERNCED BY TAG NAME
-- FOR NOW, ONLY ARRAYS OF PRIMITIVES CAN BE RETURNED
-- FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS array;;
CREATE FUNCTION array (
	value		VARCHAR(65000) character set latin1,
	tag			VARCHAR(40)
) RETURNS
	varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);

	SET begin_tag=concat("\"", tag, "\":");
	IF instr(value, begin_tag)=0 THEN
		RETURN NULL;
	ELSE
		RETURN concat("[", string.between(substring(value, instr(value, begin_tag)+length(begin_tag)-1, 65000), "[", "]", 1), "]");
	END IF;
END;;


-- RETURN A NUMERIC VALUE AT ARRAY INDEX
-- FINDS FIRST INSTANCE OF AN ARRAY WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS arrayn;;
CREATE FUNCTION arrayn (
	value		VARCHAR(65000) character set latin1,
	index_		INTEGER
) RETURNS varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	RETURN trim(string.get_word(string.between(value, "[", "]", 1), ",", index_));
END;;


DROP FUNCTION IF EXISTS slice;;
CREATE FUNCTION slice(
	value		VARCHAR(65000) character set latin1,
	start_		INTEGER,
	end_		INTEGER
)
	RETURNS VARCHAR(65000) character set latin1
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE n INTEGER;
	DECLARE s INTEGER;
	DECLARE e INTEGER;

	IF end_=start_ THEN RETURN "[]"; END IF;

	SET n=start_;
	SET s=LOCATE("[", value)+1;
	ls: LOOP
		IF n=0 THEN
			LEAVE ls;
		END IF;
		SET s=LOCATE(",", value, s)+1;
		IF s=0 THEN
			RETURN "[]";
		END IF;
		SET n=n-1;
	END LOOP ls;

	SET n=end_-start_;
	SET e=s-1;
	le: LOOP
		IF n=0 THEN
			RETURN concat("[", trim(substring(value, s, e-s)), "]");
		END IF;
		SET e=LOCATE(",", value, e+1);
		IF e=0 THEN
			SET e=LOCATE("]", value, s);
			RETURN concat("[", substring(value, s, e-s), "]");
		END IF;
		SET n=n-1;
	END LOOP;
END;;

SELECT slice("[23, 45, 32, 44, 99]", 1,3) from dual;;
SELECT slice("[23, 45, 32, 44, 99]", 0,3) from dual;;
SELECT slice("[23, 45, 32, 44, 99]", 0,0) from dual;;
SELECT slice("[23, 45, 32, 44, 99]", 0,9) from dual;;

