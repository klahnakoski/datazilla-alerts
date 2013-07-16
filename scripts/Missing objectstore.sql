DELIMITER;;

USE ekyle_objectstore_1;;


SET @ROWNUM=-1;;





SELECT 
	@ROWNUM:=@ROWNUM+1 `index`,
	id,
	`end`
FROM
	(
	SELECT STRAIGHT_JOIN 
		a.id,
		CASE WHEN b.id IS NULL THEN "max" ELSE "min" END `end`
	FROM
		objectstore a
	LEFT JOIN
		objectstore b ON b.id=a.id+1
	LEFT JOIN
		objectstore c ON c.id=a.id-1
	WHERE
		b.id IS NULL OR
		c.id IS NULL
	ORDER BY
		a.id
	) a
;;