SELECT EMPLOYEES.eid ,EMPLOYEES.ename,EMPLOYEES.salary
FROM SCHEDULE,EMPLOYEES,CERTIFIED
WHERE SCHEDULE. aid=CERTIFIED. aid ,CERTIFIED. eid=EMPLOYEES. eid