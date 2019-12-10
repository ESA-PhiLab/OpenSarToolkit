from shapely.geometry import box


def get_bursts_by_polygon(master_annotation, out_poly=None):
    master_bursts = master_annotation

    bursts_dict = {'IW1': [], 'IW2': [], 'IW3': []}
    for subswath, nr, id, b in zip(
            master_bursts['SwathID'],
            master_bursts['BurstNr'],
            master_bursts['AnxTime'],
            master_bursts['geometry']
    ):

        # Return all burst combinations if out poly is None
        if out_poly is None:
            if (nr, id) not in bursts_dict[subswath]:
                b_bounds = b.bounds
                burst_buffer = abs(b_bounds[2]-b_bounds[0])/75
                burst_bbox = box(
                    b_bounds[0], b_bounds[1], b_bounds[2], b_bounds[3]
                ).buffer(burst_buffer).envelope
                bursts_dict[subswath].append((nr, id, burst_bbox))
        elif b.intersects(out_poly):
            if (nr, id) not in bursts_dict[subswath]:
                b_bounds = b.bounds
                burst_buffer = abs(out_poly.bounds[2]-out_poly.bounds[0])/75
                burst_bbox = box(
                    b_bounds[0], b_bounds[1], b_bounds[2], b_bounds[3]
                ).buffer(burst_buffer).envelope
                bursts_dict[subswath].append((nr, id, burst_bbox))
    return bursts_dict


def get_bursts_pairs(master_annotation, slave_annotation, out_poly=None):
    master_bursts = master_annotation
    slave_bursts = slave_annotation

    bursts_dict = {'IW1': [], 'IW2': [], 'IW3': []}
    for subswath, nr, id, b in zip(
            master_bursts['SwathID'],
            master_bursts['BurstNr'],
            master_bursts['AnxTime'],
            master_bursts['geometry']
    ):
        for sl_subswath, sl_nr, sl_id, sl_b in zip(
                slave_bursts['SwathID'],
                slave_bursts['BurstNr'],
                slave_bursts['AnxTime'],
                slave_bursts['geometry']
        ):
            # Return all burst combinations if out poly is None
            if out_poly is None and b.intersects(sl_b):
                if subswath == sl_subswath and \
                        (nr, id, sl_nr, sl_id) not in bursts_dict[subswath]:
                    b_bounds = b.union(sl_b).bounds
                    burst_buffer = abs(b_bounds[2]-b_bounds[0])/75
                    burst_bbox = box(
                        b_bounds[0], b_bounds[1], b_bounds[2], b_bounds[3]
                    ).buffer(burst_buffer).envelope
                    bursts_dict[subswath].append((nr, id, sl_nr, sl_id, burst_bbox))
            elif b.intersects(sl_b) \
                    and b.intersects(out_poly) and sl_b.intersects(out_poly):
                if subswath == sl_subswath and \
                        (nr, id, sl_nr, sl_id) not in bursts_dict[subswath]:
                    b_bounds = b.union(sl_b).bounds
                    burst_buffer = abs(out_poly.bounds[2]-out_poly.bounds[0])/75
                    burst_bbox = box(
                        b_bounds[0], b_bounds[1], b_bounds[2], b_bounds[3]
                    ).buffer(burst_buffer).envelope
                    bursts_dict[subswath].append((nr, id, sl_nr, sl_id, burst_bbox))
    return bursts_dict
