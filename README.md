##### transfer-from-ssdb-to-ssdb

Скрипт предназначен для переноса событий click и open  из SSDBHistory в SSDBActionsHistory за последние 90 дней.

|**Cmd:**     | ./ssdb-history-to-ssdb-actions-history -stderrthreshold info |
| ---:        | --- |
|**Args:**    | -b (2000 default) - batch size  |
              | -wp (6 default) - колличество запущенных worker для парсинга |
              | -ww (6 default) - колличество запущенных worker для записи в ssdb |
              | -r (false default) - readonly mode |