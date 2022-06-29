CREATE OR REPLACE FUNCTION F_COMPUTE_TILE_INDICES(TS double, WINDOW_SIZE double, FREQUENCY double, BLIND_SPOT double, TIME_MODULO_FREQUENCY double)
    RETURNS VARIANT
    LANGUAGE JAVASCRIPT
    AS
    $$
    var last_tile_index = Math.floor((TS - TIME_MODULO_FREQUENCY) / FREQUENCY); // Note: exclusive
    var num_tiles = Math.floor(WINDOW_SIZE / FREQUENCY);
    var first_tile_index = last_tile_index - num_tiles;
    var tile_indices = [...Array(num_tiles).keys()].map(i => i + first_tile_index)
    return tile_indices;
    $$
