--RTP  --> Do GD config
---------------------------------------------------------------------------------------------------


--Actual RTP Công thuc (tong tien thang/tong tien cuoc)x100%
SELECT
    SUM(money_win * exchange_rate) AS total_money_win,
    SUM(money_bet * exchange_rate) AS total_money_bet,
    ROUND((SUM(money_win * exchange_rate) / NULLIF(SUM(money_bet * exchange_rate), 0)) * 100, 2) AS actual_rtp
FROM Fact_RNG_Bets b
join Dim_Currency c on b.currency = c.currency_code
WHERE service_id = "5998" 
  AND bet_date BETWEEN '2025-06-30' AND '2025-07-06';

--So sanh Actual RTP ky nay so voi ky truoc [RTP thực tế - RTP target]

---------------------------------------------------------------------------------------------------


--WIN RATE (Tong so van thang/Tong so van choi) * 100%     
               
--Tong so van thang 8
SELECT 
    service_id, play_session_id, gate_id, bet_time,
    SUM(money_win) AS total_money_win,
    SUM(money_bet) AS total_money_bet,
    SUM(money_back) AS total_money_back,
    SUM(money_win) - SUM(money_bet) + SUM(money_back) AS win_lose
FROM Fact_RNG_Bets
WHERE service_id = '3992' AND bet_time BETWEEN '2025-06-04' AND '2025-06-07'
GROUP BY service_id, play_session_id
HAVING (SUM(money_win) - SUM(money_bet) + SUM(money_back)) > 0;

--Tong so van choi 33
SELECT 
    service_id, play_session_id,gate_id, bet_time,
    SUM(money_win) AS total_money_win,
    SUM(money_bet) AS total_money_bet,
    SUM(money_back) AS total_money_back,
    SUM(money_win) - SUM(money_bet) + SUM(money_back) AS win_lose
FROM Fact_RNG_Bets
WHERE service_id = '3992' AND bet_time BETWEEN '2025-06-04' AND '2025-06-07'
GROUP BY service_id, play_session_id;

--So sanh win rate (ky nay- ky truoc)

---------------------------------------------------------------------------------------------------
--Player Retention (D1)


WITH player_daily AS (
  SELECT 
    session_start_date,
    player_id,
    service_id
  FROM Fact_RNG_PlayerDailyActivity
  WHERE service_id = 3974
    AND session_start_date BETWEEN '2025-07-01' AND '2025-07-31'
),

date_pairs AS (
  SELECT 
    today.session_start_date AS today_date,
    yesterday.session_start_date AS yesterday_date,
    today.player_id
  FROM player_daily today
  JOIN player_daily yesterday
    ON today.player_id = yesterday.player_id
    AND today.session_start_date = DATE_ADD(yesterday.session_start_date, INTERVAL 1 DAY)
),

daily_results AS (
  SELECT 
    today_date,
    COUNT(DISTINCT player_id) AS return_users,
    (SELECT COUNT(DISTINCT player_id)
     FROM player_daily pd
     WHERE pd.session_start_date = dp.today_date - INTERVAL 1 DAY) AS total_users_today,
    ROUND(
      COUNT(DISTINCT player_id) * 1.0 /
      (SELECT COUNT(DISTINCT player_id)
       FROM player_daily pd
       WHERE pd.session_start_date = dp.today_date - INTERVAL 1 DAY)
      * 100, 2
    ) AS return_rate_percent
  FROM date_pairs dp
  GROUP BY today_date
)

SELECT 
  today_date,
  return_users,
  total_users_today,
  return_rate_percent,
  (SELECT ROUND(SUM(return_rate_percent) / 31, 2) FROM daily_results) AS total_return_rate_percent   --De y cho nay neu thoi gian 30 thì chia 30, 31 thi chia 31
FROM daily_results;


--So sanh nguoi choi quay lai [(kỳ này/kỳ trước) - 1]*100%

---------------------------------------------------------------------------------------------------


--Avg Session Time (Tong thoi gian choi/Tong so user choi game)
SELECT 
    COUNT(DISTINCT player_id) AS tong_so_user_choi,
    SUM(duration_session) AS tong_so_tg_user_choi,
    SUM(duration_session) / COUNT(DISTINCT player_id) AS avg_session_time
FROM 
    Fact_RNG_PlayerActivity 
WHERE 
    service_id = 3974 
    AND session_start_date BETWEEN '2025-07-01' AND '2025-07-31';
    
--So sanh Avg Session Time [(kỳ này/kỳ trước) - 1]*100%


---------------------------------------------------------------------------------------------------


--Playing Users (Dem tong so user duy nhat co cuoc trong ky)

SELECT 
    COUNT(DISTINCT player_id) AS tong_so_user_co_hanh_dong_cuoc
FROM 
    Fact_RNG_PlayerActivity
WHERE 
    session_start_date BETWEEN '2025-07-01' AND '2025-07-31' and service_id = 3974;
    
--So sanh Playing Users [(kỳ này/kỳ trước) - 1]*100%

---------------------------------------------------------------------------------------------------

--Vi trung binh (Tong vi cua tat ca nguoi choi truoc 1 van choi/ Tong so nguoi choi)

WITH first_wallet_txn_per_session AS (
    SELECT *
    FROM (
      SELECT 
        player_id,
        service_id,
        game_round_id,
        timestamp,
        wallet_balance_before,
        ROW_NUMBER() OVER (
          PARTITION BY player_id, game_round_id
          ORDER BY timestamp
        ) AS rn
      FROM Fact_Wallet_Transaction
      WHERE service_id = 3932
        AND date_key BETWEEN '2025-05-01' AND '2025-05-31'
        AND wallet_balance_before IS NOT NULL
    ) AS tmp
    WHERE rn = 1
),
player_avg_balance AS (
    SELECT
        player_id,
        AVG(wallet_balance_before) AS avg_balance_per_player
    FROM first_wallet_txn_per_session
    GROUP BY player_id
)
SELECT 
    AVG(avg_balance_per_player) AS overall_avg_balance_per_player,
    COUNT(DISTINCT player_id) AS total_unique_players
FROM player_avg_balance;




--Tong vi tat ca nguoi choi truoc 1 van choi
SELECT AVG(wallet_balance_before) as average_wallet_balance
FROM (
    SELECT wallet_balance_before,
           ROW_NUMBER() OVER (PARTITION BY player_id, game_round_id ORDER BY timestamp) as rn
    FROM Fact_Wallet_Transaction
    WHERE service_id = 3932
    AND date_key BETWEEN '2025-05-01' AND '2025-05-31'
) t
WHERE rn = 1;



--Tong so nguoi choi
SELECT 
    COUNT(DISTINCT player_id) AS tong_so_user_choi
    FROM Fact_Wallet_Transaction WHERE service_id = 3932
    AND date_key BETWEEN '2025-05-01' AND '2025-05-31'
    
    
--So sanh vi trung binh [(kỳ này/kỳ trước) - 1]*100%


---------------------------------------------------------------------------------------------------


--BET TRUNG BINH (Tong tien cuoc / so van choi)

SELECT 
    COUNT(DISTINCT play_session_id) AS distinct_game_count,
    SUM(money_bet) AS total_money_bet,
    SUM(money_bet) / COUNT(DISTINCT play_session_id) AS avg_bet_per_distinct_game
FROM 
    Fact_RNG_Bets where bet_date BETWEEN '2025-05-01' AND '2025-05-31' and service_id = 3932
    
--So sanh bet trung binh [(kỳ này/kỳ trước) - 1]*100%

---------------------------------------------------------------------------------------------------

--CHART XU HUONG HANH VI NGUOI CHOI
--Total user (dang tinh total user luy tuyen)

SELECT 
    t.*,
    (SELECT COUNT(DISTINCT player_id) 
     FROM Fact_RNG_PlayerActivity 
     WHERE service_id = 3932 
     AND session_start_date between '2025-05-01' and '2025-05-19') as total_user
FROM Fact_RNG_PlayerActivity t
WHERE service_id = 3932 
AND session_start_date between '2025-05-01' and '2025-05-19';

--New Users and Returning

WITH daily_players AS (
    SELECT 
        bet_date,
        player_id
    FROM Fact_RNG_Bets
    WHERE bet_date BETWEEN '2025-05-01' AND '2025-05-31'
      AND service_id = 3932
    GROUP BY bet_date, player_id
),
first_bet_dates AS (
    SELECT 
        player_id,
        MIN(bet_date) AS first_bet_date
    FROM Fact_RNG_Bets
    WHERE service_id = 3932
    GROUP BY player_id
),
classified_users AS (
    SELECT 
        dp.bet_date,
        dp.player_id,
        CASE 
            WHEN fbd.first_bet_date = dp.bet_date THEN 'new'
            WHEN fbd.first_bet_date < dp.bet_date THEN 'return'
        END AS user_type
    FROM daily_players dp
    JOIN first_bet_dates fbd ON dp.player_id = fbd.player_id
)
SELECT 
    bet_date,
    COUNT(CASE WHEN user_type = 'new' THEN 1 END) AS new_users,
    COUNT(CASE WHEN user_type = 'return' THEN 1 END) AS return_users
FROM classified_users
GROUP BY bet_date
ORDER BY bet_date;

---------------------------------------------------------------------------------------------------

--TI LE THANG THUA
--Ti le thang (So van thang/tong so van choi)*100%
--Ti le thua [(So van thua + hoa)/tong so van choi]*100%
--Tong so van thang 152
SELECT 
    service_id, play_session_id, gate_id, bet_time,
    SUM(money_win) AS total_money_win,
    SUM(money_bet) AS total_money_bet,
    SUM(money_back) AS total_money_back,
    SUM(money_win) - SUM(money_bet) + SUM(money_back) AS win_lose
FROM Fact_RNG_Bets
WHERE service_id = '3932' AND bet_time BETWEEN '2025-05-01' AND '2025-05-31'
GROUP BY service_id, play_session_id
HAVING (SUM(money_win) - SUM(money_bet) + SUM(money_back)) > 0;

--Tong so van choi 511
SELECT 
    service_id, play_session_id,gate_id, bet_time,
    SUM(money_win) AS total_money_win,
    SUM(money_bet) AS total_money_bet,
    SUM(money_back) AS total_money_back,
    SUM(money_win) - SUM(money_bet) + SUM(money_back) AS win_lose
FROM Fact_RNG_Bets
WHERE service_id = '3932' AND bet_time BETWEEN '2025-05-01' AND '2025-05-31'
GROUP BY service_id, play_session_id;

---------------------------------------------------------------------------------------------------

--HANH VI NGUOI CHOI

WITH MonthlyStats AS (
    SELECT 
        SUM(total_winning_bets) AS total_wins,
        SUM(quits_after_win) AS wins_quit,
        SUM(continues_after_win) AS wins_continue,
        SUM(total_losing_bets) AS total_losses,
        SUM(quits_after_lose) AS losses_quit,
        SUM(continues_after_lose) AS losses_continue
    FROM Fact_RNG_Daily_WinLoseBehavior
    WHERE service_id = 3932
    AND bet_date BETWEEN '2025-05-01' AND '2025-05-31'
)

SELECT 
    -- Ti le thang choi tiep
    ROUND(wins_continue * 100.0 / NULLIF(total_wins, 0), 2) AS win_continue_pct,
    
    -- Ti le thang thoat
    ROUND(wins_quit * 100.0 / NULLIF(total_wins, 0), 2) AS win_quit_pct,
    
    -- Ti le thua choi tiep
    ROUND(losses_continue * 100.0 / NULLIF(total_losses, 0), 2) AS lose_continue_pct,
    
    -- Ti le thua thoat
    ROUND(losses_quit * 100.0 / NULLIF(total_losses, 0), 2) AS lose_quit_pct
FROM MonthlyStats;


---------------------------------------------------------------------------------------------------

--ML HANH VI NGUOI CHOI

WITH WinBehavior AS (
    SELECT 
        'Thang' AS Ket_qua,
        CASE 
            WHEN continues_after_win > 0 THEN 'Choi'
            ELSE 'Thoat'
        END AS Hanh_vi,
        COUNT(DISTINCT b.player_id) AS So_luong,
        SUM(money_bet) AS Tong_cuoc
    FROM Fact_RNG_Bets b
    JOIN Fact_RNG_Daily_WinLoseBehavior w ON b.service_id = w.service_id AND b.bet_date = w.bet_date
    WHERE money_win > money_bet
    GROUP BY CASE 
            WHEN continues_after_win > 0 THEN 'Choi'
            ELSE 'Thoat'
        END
),
LoseBehavior AS (
    SELECT 
        'Thua' AS Ket_qua,
        CASE 
            WHEN continues_after_lose > 0 THEN 'Choi'
            ELSE 'Thoat'
        END AS Hanh_vi,
         COUNT(DISTINCT b.player_id) AS So_luong,
        SUM(money_bet) AS Tong_cuoc
    FROM Fact_RNG_Bets b
    JOIN Fact_RNG_Daily_WinLoseBehavior w ON b.service_id = w.service_id AND b.bet_date = w.bet_date
    WHERE money_win <= money_bet
    GROUP BY CASE 
            WHEN continues_after_lose > 0 THEN 'Choi'
            ELSE 'Thoat'
        END
),
Combined AS (
    SELECT * FROM WinBehavior
    UNION ALL
    SELECT * FROM LoseBehavior
),
TotalCount AS (
    SELECT SUM(So_luong) AS total FROM Combined
)
SELECT 
    c.Ket_qua,
    c.Hanh_vi,
    c.So_luong,
    CONCAT(ROUND(c.So_luong * 100.0 / t.total, 2), '%') AS Ti_le,
    CONCAT(ROUND(c.Tong_cuoc / 1000000, 2), 'M') AS Tong_cuoc
FROM Combined c
CROSS JOIN TotalCount t
ORDER BY c.Ket_qua DESC, c.Hanh_vi DESC;



---------------------------------------------------------------------------------------------------