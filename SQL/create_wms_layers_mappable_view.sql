create view discovery.wms_layers_mappable as
select
s.*,
st_makeenvelope(s.min_x, s.min_y, s.max_x, s.max_y, 4326) AS geom
from
(
 select
 id as gid,
 csw_url,
 csw_record_identifier,
 csw_record_publisher,
 csw_record_title,
 csw_record_subjects,
 csw_record_abstract,
 csw_record_modified,
 wms_url,
 wms_url_domain,
 wms_layer_for_record_title,
 wms_layer_for_record_name,
 wms_access_constraints,
 only_1_choice,
 match_dist,
 bbox_wgs84,
 bbox_projected,
 wms_get_cap_error,
 wms_get_map_error,
 made_get_map_req,
 image_status,
 out_image_fname,
 (split_part(substring(bbox_wgs84, 2, length(bbox_wgs84)-2), ',', 1))::double precision as min_x,
 (split_part(substring(bbox_wgs84, 2, length(bbox_wgs84)-2), ',', 2))::double precision as min_y,
 (split_part(substring(bbox_wgs84, 2, length(bbox_wgs84)-2), ',', 3))::double precision as max_x,
 (split_part(substring(bbox_wgs84, 2, length(bbox_wgs84)-2), ',', 4))::double precision as max_y
 from discovery.wms_layers
) s;