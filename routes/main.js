module.exports = function (app) {


    // Homepage Route
    // @param req is request object
    // @param res is response request
    // Output: index.html is rendered
    app.get("/", function (req, res) {
        res.render("index.html")
    });

    // Players by points list view Route
    // @param req is request object
    // @param res is response request
    // Output: players-points.html is rendered
    app.get("/players/points", function (req, res) {

        // Query database to get list of devices to view
        let sqlquery = `SELECT players.name, players.id, 
                        players.jersey_number,
                            teams.name AS team_name,
                                teams.alias,
                                teams.market,
                                SUM(shots.points) as total_points
                        FROM players
                        LEFT JOIN teams ON players.team_id = teams.id
                        LEFT JOIN events ON players.id = events.player_id
                        LEFT JOIN event_types ON events.id = event_types.event_id
                        LEFT JOIN shots ON event_types.shot_id = shots.id
                        GROUP BY 1, 2, 3, 4, 5, 6
                        ORDER BY total_points DESC LIMIT 50;`;

        // Execute sql query
        db.query(sqlquery, (err, result) => {
            if (err) {
                res.redirect("index.html", { message: { type: "warning", body: "Players were not found" } });
            }
            // Passing all devices
            res.render("players-points.html", { players: result });
        });
    });

    // Top rebounding players list view Route
    // @param req is request object
    // @param res is response request
    // Output: players-rebounds.html is rendered
    app.get("/players/rebounds", function (req, res) {

        // Query database to get list of players with most rebounds
        let sqlquery = `SELECT players.name, players.id,
                    players.jersey_number, 
                    teams.name AS team_name, 
                    teams.alias, 
                    teams.market, 
                    COUNT(event_types.rebound_id) as rebounds,
                    SUM(CASE WHEN rebounds.type='offensive' THEN 1 ELSE 0 END) AS offensive_rebounds,
                    SUM(CASE WHEN rebounds.type='defensive' THEN 1 ELSE 0 END) AS defensive_rebounds
                FROM players 
                LEFT JOIN teams ON players.team_id = teams.id 
                LEFT JOIN events ON players.id = events.player_id 
                LEFT JOIN event_types ON events.id = event_types.event_id 
                RIGHT JOIN rebounds ON event_types.rebound_id = rebounds.id 
                GROUP BY 1, 2, 3, 4, 5, 6
                ORDER BY rebounds DESC LIMIT 50;`;

        // Execute sql query
        db.query(sqlquery, (err, result) => {
            if (err) {
                res.redirect("index.html", { message: { type: "warning", body: "Players were not found" } });
            }
            // Passing top rebounding players
            res.render("players-rebounds.html", { players: result });
        });
    });

    // Top venues by Attendance Route
    // @param req is request object
    // @param res is response request
    // Output: venues.html is rendered
    app.get("/venues", function (req, res) {

        // Query database to get list of venues with highest attendance
        let sqlquery = `SELECT
                    venues.name, venues.city, venues.state, venues.capacity, AVG(games.attendance) as attendance_avg
                    FROM games
                    LEFT JOIN venues ON games.venue_id = venues.id
                    GROUP BY 1, 2, 3, 4
                    ORDER BY attendance_avg DESC
                    LIMIT 50;`;

        // Execute sql query
        db.query(sqlquery, (err, result) => {
            if (err) {
                res.redirect("index.html", { message: { type: "warning", body: "Venues were not found" } });
            }
            // Passing top rebounding players
            res.render("venues.html", { venues: result });
        });
    });


    // List divisions
    // @param req is request object
    // @param res is response request
    // Output: venues.html is rendered
    app.get("/divisions", function (req, res) {

        // Query database to get list of divisions
        let sqlquery = `SELECT
                    name, id
                    FROM divisions
                    ;`;

        // Execute sql query
        db.query(sqlquery, (err, result) => {
            if (err) {
                res.redirect("index.html", { message: { type: "warning", body: "Divisions were not found" } });
            }
            // Passing top rebounding players
            res.render("divisions.html", { divisions: result });
        });
    });


    // List conferences
    // @param req is request object
    // @param res is response request
    // Output: venues.html is rendered
    app.get("/division/:id", function (req, res) {

        // Query database to get list of divisions
        let sqlquery = `SELECT
                    conferences.name, conferences.id, conferences.alias, divisions.name AS division_name
                    FROM conferences 
                    INNER JOIN divisions ON conferences.division_id = divisions.id
                    AND conferences.division_id=?
                    ;`;

        // Execute sql query
        db.query(sqlquery, req.params['id'], (err, result) => {
            if (err) {
                res.render("index.html", { message: "Division was not found; please try again." });
            }
            // Passing first result (should only be one) of the query as device object
            res.render("conferences.html", { conferences: result, division: result[0].division_name });
        });
    });
    // List teams in a conference
    // @param req is request object
    // @param res is response request
    // Output: venues.html is rendered
    app.get("/conference/:id", function (req, res) {

        // Query database to get list of divisions
        let sqlquery = `SELECT
                    teams.market, teams.alias, teams.name as team_name, teams.id,
                    conferences.name as conference_name, conferences.alias as conference_alias
                    FROM teams 
                    INNER JOIN conferences ON teams.conference_id = conferences.id
                    AND teams.conference_id=?
                    ;`;

        // Execute sql query
        db.query(sqlquery, req.params['id'], (err, result) => {
            if (err) {
                res.render("index.html", { message: "Division was not found; please try again." });
            }
            // Passing first result (should only be one) of the query as device object
            res.render("teams.html", { teams: result, conference: result[0] });
        });
    });


    // Player Route
    // Player id is used to show data re. player the user is attempting to view
    // @param req is request object
    // @param res is response request
    // Output: player.html is rendered
    app.get("/player/:id", function (req, res) {

        // Query database to get specific device user is attempting to view
        // Device to return is determined by device id which is passed as :id param in route
        let sqlquery = `SELECT players.name AS player_name, players.jersey_number, teams.name AS team_name, teams.market, teams.alias, teams.id AS team_id,
                    SUM(shots.points) AS total_points, 
                    COUNT(event_types.rebound_id) as total_rebounds,
                    COUNT(event_types.assist_id) AS total_assists,
                    COUNT(event_types.turnover_id) AS total_turnovers,
                    COUNT(shots.made IS NULL) AS shots_attempted,
                    SUM(shots.made) AS shots_made,
                    SUM(shots.made) / COUNT(shots.made IS NULL) * 100 AS shooting_percentage
                    FROM players
                    INNER JOIN teams ON players.team_id = teams.id AND players.id=?
                    LEFT JOIN events ON players.id = events.player_id
                    LEFT JOIN event_types ON events.id = event_types.event_id
                    LEFT JOIN shots ON event_types.shot_id = shots.id
                    GROUP BY 1, 2, 3, 4, 5, 6
                    ;`;

        // Execute sql query
        db.query(sqlquery, req.params['id'], (err, result) => {
            if (err) {
                res.render("index.html", { message: "Device was not found; please try again." });
            }
            console.log(result)
            // Passing first result (should only be one) of the query as device object
            res.render("player.html", { player: result[0] });
        });
    });

    // Team Route
    // Team id is used to show data re. team the user is attempting to view
    // @param req is request object
    // @param res is response request
    // Output: team.html is rendered
    app.get("/team/:id", function (req, res) {

        // Query database to get specific device user is attempting to view
        // Device to return is determined by device id which is passed as :id param in route
        let sqlquery = `SELECT players.name AS player_name, players.id as player_id, players.jersey_number, teams.name AS team_name, teams.market, teams.alias,
                        SUM(shots.points) AS total_points,
                        COUNT(event_types.rebound_id) as total_rebounds,
                        COUNT(event_types.assist_id) AS total_assists,
                        COUNT(event_types.turnover_id) AS total_turnovers
                        FROM players
                        INNER JOIN teams ON players.team_id = teams.id AND teams.id =?
                        LEFT JOIN events ON players.id = events.player_id
                        LEFT JOIN event_types ON events.id = event_types.event_id
                        LEFT JOIN shots ON event_types.shot_id = shots.id
                        GROUP BY 1, 2, 3, 4, 5, 6
                        ORDER BY total_points DESC
                        ;`;

        // Execute sql query
        db.query(sqlquery, req.params['id'], (err, result) => {
            if (err) {
                res.render("index.html", { message: "Team was not found; please try again." });
            }
            console.log(result[0], result)
            // Passing first result (should only be one) of the query as device object
            res.render("team.html", { team: result[0], players: result });
        });
    });
}