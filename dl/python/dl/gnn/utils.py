#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

import tensorflow as tf
import graphlearn as gl


def _feat_types_and_shapes(feat_spec, sparse=False):
    """Get types and shapes of FeatSpec.
    Args:
      feat_spec: A FeatureSpec object used to parse the feature.
      sparse: Bool, set to true if the feature is in the sparse format.
    Returns:
      two list of TF types and shapes.
    """
    # ids
    output_types = [tf.int64]
    output_shapes = [tf.TensorShape([None])]
    # sparse
    if sparse:
        # offsets, dense_shape, indices
        output_types.extend([tf.int64, tf.int64, tf.int64])
        output_shapes.extend([tf.TensorShape([None]),
                              tf.TensorShape([2]),
                              tf.TensorShape([None, 2])])
    # labels
    if feat_spec.labeled:
        output_types.extend([tf.int32])
        output_shapes.extend([tf.TensorShape([None])])
    # weights
    if feat_spec.weighted:
        output_types.extend([tf.float32])
        output_shapes.extend([tf.TensorShape([None])])
    # attributes
    if feat_spec.cont_attrs_num > 0:
        output_types.extend([tf.float32])
        output_shapes.extend([tf.TensorShape([None, feat_spec.cont_attrs_num])])
    if feat_spec.cate_attrs_num > 0:
        output_types.extend([tf.string])
        output_shapes.extend([tf.TensorShape([None, feat_spec.cate_attrs_num])])

    return output_types, output_shapes


def ego_types_and_shapes(ego_spec):
    """Get types and shapes of EgoSpec.

    Args:
      ego_spec: Ego spec.
      append_label: whether append label
    Returns:
      Two tuple of types and shapes
    """
    if ego_spec is None:
        return tuple(), tuple()

    # src(root), Nodes or Edges
    output_types, output_shapes = _feat_types_and_shapes(ego_spec.src_spec)
    # neighbors
    if ego_spec.hops_spec is None:
        return tuple(output_types), tuple(output_shapes)

    for i in range(len(ego_spec.hops_spec)):
        # Nodes
        if ego_spec.hops_spec[i].node_spec is not None:
            nbr_ego_types, nbr_ego_shapes = _feat_types_and_shapes(
                ego_spec.hops_spec[i].node_spec,
                sparse=ego_spec.hops_spec[i].sparse)
            output_types.extend(nbr_ego_types)
            output_shapes.extend(nbr_ego_shapes)
        # Edges
        if ego_spec.hops_spec[i].edge_spec is not None:
            nbr_ego_types, nbr_ego_shapes = _feat_types_and_shapes(
                ego_spec.hops_spec[i].edge_spec,
                sparse=ego_spec.hops_spec[i].sparse)
            output_types.extend(nbr_ego_types)
            output_shapes.extend(nbr_ego_shapes)

    return tuple(output_types), tuple(output_shapes)


def edge_ori(edge_segments):
    if edge_segments[0] == '<-' and edge_segments[-1] == '-':
        return -1
    if edge_segments[0] == '-' and edge_segments[-1] == '->':
        return 1
    raise ValueError("Illegal edge def: %s[%s]%s" % edge_segments)


def load_graph(init_redis=False):
    kwargs = {
        "gp_host": "192.168.8.138",
        "gp_port": 35432,
        "gp_user": "gpadmin",
        "gp_dbname": "dev",
        "redis_host": "192.168.8.138",
        "redis_port": 16379,
        "init_redis": init_redis
    }
    g = gl.Graph(**kwargs) \
        .node("", node_type="1",
              decoder=gl.Decoder(labeled=True, label_column="label",
                                 attr_types=["int", "string", "int", ],
                                 attr_columns=["limit", "level", "bill"])) \
        .node("", node_type="v",
              decoder=gl.Decoder(labeled=True, label_column="label",
                                 attr_types=["int", "string", "int", ],
                                 attr_columns=["limit", "level", "bill"])) \
        .node("", node_type="t",
              decoder=gl.Decoder(labeled=True, label_column="label",
                                 attr_types=["int", "string", "int", ],
                                 attr_columns=["limit", "level", "bill"])) \
        .node("", node_type="2",
              decoder=gl.Decoder(attr_types=["string", "string"],
                                 attr_columns=["type", "city"])) \
        .node("", node_type="3",
              decoder=gl.Decoder(attr_types=["string"],
                                 attr_columns=["type"])) \
        .edge("",
              edge_type=("1", "1", "1"),
              decoder=gl.Decoder(weighted=True, weight_column="weight"), directed=False) \
        .edge("",
              edge_type=("1", "2", "2"),
              decoder=gl.Decoder(weighted=True, weight_column="weight"), directed=False) \
        .edge("",
              edge_type=("1", "3", "3"),
              decoder=gl.Decoder(weighted=True, weight_column="weight"), directed=False)
    return g.init()