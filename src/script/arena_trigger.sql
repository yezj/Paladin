CREATE OR REPLACE FUNCTION create_userarena()
  RETURNS "trigger" AS
$BODY$begin
  if TG_OP = 'UPDATE' then
    if old.xp < 900000 and new.xp >= 900000 then
      insert into core_arena(user_id, arena_coin, before_rank, now_rank, last_rank, jguards, jpositions, formation, timestamp) VALUES (old.id, 0, currval('core_arena_id_seq'), currval('core_arena_id_seq'), currval('core_arena_id_seq'),'{"01001":' || ((new.jheros)::json->'01001') || ',"01002":' || ((new.jheros)::json->'01002') || '}', '{"01001": "-1","01002":"-1"}', 1, trunc(extract(epoch from now())));
    end if;
  end if;
  return new;
end;$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION create_userarena() OWNER TO deploy;

CREATE TRIGGER tg_create_userarena
  BEFORE UPDATE
  ON core_user
  FOR EACH ROW
  EXECUTE PROCEDURE create_userarena();