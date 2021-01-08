DELETE FROM sapient.DATE_TIME_MAX_USERS WHERE date = '{{ dag_run.conf['date'] }}';

INSERT INTO sapient.DATE_TIME_MAX_USERS
SELECT date, hour, count_users FROM
(
	SELECT date, hour, count_users, RANK()
	OVER (
		PARTITION BY date
		ORDER BY count_users DESC
	) AS RANK_TO_EACH_HOUR
	FROM
	(
		Select EXTRACT(DATE FROM date) as date,
		EXTRACT(HOUR FROM date) as hour,
		count(user_id) as count_users from sapient.USER_ACTIVITY
		where EXTRACT(DATE from date) = '{{ dag_run.conf['date'] }}'
		group by date, hour
	)
) WHERE RANK_TO_EACH_HOUR = 1 
ORDER by date