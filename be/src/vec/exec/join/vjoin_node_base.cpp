// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/exec/join/vjoin_node_base.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/tracer.h>
#include <stddef.h>

#include <sstream>
#include <system_error>

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/telemetry/telemetry.h"
#include "util/threadpool.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {
class ObjectPool;
} // namespace doris

namespace doris::vectorized {

VJoinNodeBase::VJoinNodeBase(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _join_op(tnode.__isset.hash_join_node ? tnode.hash_join_node.join_op
                                                : (tnode.__isset.nested_loop_join_node
                                                           ? tnode.nested_loop_join_node.join_op
                                                           : TJoinOp::CROSS_JOIN)),
          _have_other_join_conjunct(tnode.__isset.hash_join_node &&
                                    ((tnode.hash_join_node.__isset.other_join_conjuncts &&
                                      !tnode.hash_join_node.other_join_conjuncts.empty()) ||
                                     tnode.hash_join_node.__isset.vother_join_conjunct)),
          _match_all_probe(_join_op == TJoinOp::LEFT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _match_all_build(_join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _build_unique(!_have_other_join_conjunct &&
                        (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                         _join_op == TJoinOp::LEFT_ANTI_JOIN ||
                         _join_op == TJoinOp::LEFT_SEMI_JOIN)),
          _is_right_semi_anti(_join_op == TJoinOp::RIGHT_ANTI_JOIN ||
                              _join_op == TJoinOp::RIGHT_SEMI_JOIN),
          _is_left_semi_anti(_join_op == TJoinOp::LEFT_ANTI_JOIN ||
                             _join_op == TJoinOp::LEFT_SEMI_JOIN ||
                             _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN),
          _is_outer_join(_match_all_build || _match_all_probe),
          _is_mark_join(tnode.__isset.nested_loop_join_node
                                ? (tnode.nested_loop_join_node.__isset.is_mark
                                           ? tnode.nested_loop_join_node.is_mark
                                           : false)
                        : tnode.hash_join_node.__isset.is_mark ? tnode.hash_join_node.is_mark
                                                               : false),
          _short_circuit_for_null_in_build_side(_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    _init_join_op();
    if (_is_mark_join) {
        DCHECK(_join_op == TJoinOp::LEFT_ANTI_JOIN || _join_op == TJoinOp::LEFT_SEMI_JOIN ||
               _join_op == TJoinOp::CROSS_JOIN || _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN)
                << "Mark join is only supported for null aware left semi/anti join and cross join "
                   "but this is "
                << _join_op;
    }
    if (tnode.__isset.hash_join_node) {
        _output_row_desc.reset(
                new RowDescriptor(descs, {tnode.hash_join_node.voutput_tuple_id}, {false}));
        _intermediate_row_desc.reset(new RowDescriptor(
                descs, tnode.hash_join_node.vintermediate_tuple_id_list,
                std::vector<bool>(tnode.hash_join_node.vintermediate_tuple_id_list.size())));
    } else if (tnode.__isset.nested_loop_join_node) {
        _output_row_desc.reset(
                new RowDescriptor(descs, {tnode.nested_loop_join_node.voutput_tuple_id}, {false}));
        _intermediate_row_desc.reset(new RowDescriptor(
                descs, tnode.nested_loop_join_node.vintermediate_tuple_id_list,
                std::vector<bool>(tnode.nested_loop_join_node.vintermediate_tuple_id_list.size())));
    } else {
        // Iff BE has been upgraded and FE has not yet, we should keep origin logics for CROSS JOIN.
        DCHECK_EQ(_join_op, TJoinOp::CROSS_JOIN);
    }
}

Status VJoinNodeBase::close(RuntimeState* state) {
    return ExecNode::close(state);
}

void VJoinNodeBase::release_resource(RuntimeState* state) {
    VExpr::close(_output_expr_ctxs, state);
    _join_block.clear();
    ExecNode::release_resource(state);
}

void VJoinNodeBase::_construct_mutable_join_block() {
    const auto& mutable_block_desc = intermediate_row_desc();
    for (const auto tuple_desc : mutable_block_desc.tuple_descriptors()) {
        for (const auto slot_desc : tuple_desc->slots()) {
            auto type_ptr = slot_desc->get_data_type_ptr();
            _join_block.insert({type_ptr->create_column(), type_ptr, slot_desc->col_name()});
        }
    }
    if (_is_mark_join) {
        _join_block.replace_by_position(
                _join_block.columns() - 1,
                remove_nullable(_join_block.get_by_position(_join_block.columns() - 1).column));
    }
}

Status VJoinNodeBase::_build_output_block(Block* origin_block, Block* output_block) {
    auto is_mem_reuse = output_block->mem_reuse();
    if (!is_mem_reuse) {
        output_block->swap(VectorizedUtils::create_empty_columnswithtypename(row_desc()));
    }

    auto rows = origin_block->rows();
    if (rows != 0) {
        if (_output_expr_ctxs.empty()) {
            DCHECK(output_block->columns() == row_desc().num_materialized_slots());
            for (int i = 0; i < output_block->columns(); ++i) {
                if (output_block->get_by_position(i).type->is_nullable() &&
                    !origin_block->get_by_position(i).column->is_nullable()) {
                    output_block->replace_by_position(
                            i, make_nullable(origin_block->get_by_position(i).column));
                    output_block->get_by_position(i).type =
                            make_nullable(origin_block->get_by_position(i).type);
                } else {
                    output_block->replace_by_position(
                            i, std::move(origin_block->get_by_position(i).column));
                }
            }
        } else {
            DCHECK(output_block->columns() == row_desc().num_materialized_slots());
            SCOPED_TIMER(_projection_timer);
            for (int i = 0; i < output_block->columns(); ++i) {
                auto result_column_id = -1;
                RETURN_IF_ERROR(_output_expr_ctxs[i]->execute(origin_block, &result_column_id));
                auto column_ptr = origin_block->get_by_position(result_column_id)
                                          .column->convert_to_full_column_if_const();
                if (output_block->get_by_position(i).type->is_nullable() &&
                    !column_ptr->is_nullable()) {
                    output_block->replace_by_position(i, make_nullable(column_ptr));
                    output_block->get_by_position(i).type =
                            make_nullable(origin_block->get_by_position(result_column_id).type);
                } else {
                    output_block->replace_by_position(i, std::move(column_ptr));
                }
            }
        }
        DCHECK(output_block->rows() == rows);
    }

    return Status::OK();
}

Status VJoinNodeBase::init(const TPlanNode& tnode, RuntimeState* state) {
    if (tnode.__isset.hash_join_node || tnode.__isset.nested_loop_join_node) {
        const auto& output_exprs = tnode.__isset.hash_join_node
                                           ? tnode.hash_join_node.srcExprList
                                           : tnode.nested_loop_join_node.srcExprList;
        for (const auto& expr : output_exprs) {
            VExprContextSPtr ctx;
            RETURN_IF_ERROR(VExpr::create_expr_tree(expr, ctx));
            _output_expr_ctxs.push_back(ctx);
        }
    }
    // only use in outer join as the bool column to mark for function of `tuple_is_null`
    if (_is_outer_join) {
        _tuple_is_null_left_flag_column = ColumnUInt8::create();
        _tuple_is_null_right_flag_column = ColumnUInt8::create();
    }
    return ExecNode::init(tnode, state);
}

Status VJoinNodeBase::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);

    std::promise<Status> thread_status;
    try {
        state->exec_env()->join_node_thread_pool()->submit_func(
                [this, state, thread_status_p = &thread_status] {
                    this->_probe_side_open_thread(state, thread_status_p);
                });
    } catch (const std::system_error& e) {
        LOG(WARNING) << "In VJoinNodeBase::open create thread fail, " << e.what();
        return Status::InternalError(e.what());
    }

    // Open the probe-side child so that it may perform any initialisation in parallel.
    // Don't exit even if we see an error, we still need to wait for the build thread
    // to finish.
    // ISSUE-1247, check open_status after buildThread execute.
    // If this return first, build thread will use 'thread_status'
    // which is already destructor and then coredump.
    Status status = _materialize_build_side(state);
    RETURN_IF_ERROR(thread_status.get_future().get());
    RETURN_IF_ERROR(VExpr::open(_output_expr_ctxs, state));

    return status;
}

Status VJoinNodeBase::alloc_resource(doris::RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));
    RETURN_IF_ERROR(VExpr::open(_output_expr_ctxs, state));
    return Status::OK();
}

void VJoinNodeBase::_reset_tuple_is_null_column() {
    if (_is_outer_join) {
        reinterpret_cast<ColumnUInt8&>(*_tuple_is_null_left_flag_column).clear();
        reinterpret_cast<ColumnUInt8&>(*_tuple_is_null_right_flag_column).clear();
    }
}

void VJoinNodeBase::_probe_side_open_thread(RuntimeState* state, std::promise<Status>* status) {
    SCOPED_ATTACH_TASK(state);
    status->set_value(child(0)->open(state));
}

#define APPLY_FOR_JOINOP_VARIANTS(M) \
    M(INNER_JOIN)                    \
    M(LEFT_SEMI_JOIN)                \
    M(LEFT_ANTI_JOIN)                \
    M(LEFT_OUTER_JOIN)               \
    M(FULL_OUTER_JOIN)               \
    M(RIGHT_OUTER_JOIN)              \
    M(CROSS_JOIN)                    \
    M(RIGHT_SEMI_JOIN)               \
    M(RIGHT_ANTI_JOIN)               \
    M(NULL_AWARE_LEFT_ANTI_JOIN)

void VJoinNodeBase::_init_join_op() {
    switch (_join_op) {
#define M(NAME)                                                                            \
    case TJoinOp::NAME:                                                                    \
        _join_op_variants.emplace<std::integral_constant<TJoinOp::type, TJoinOp::NAME>>(); \
        break;
        APPLY_FOR_JOINOP_VARIANTS(M);
#undef M
    default:
        //do nothing
        break;
    }
}

} // namespace doris::vectorized
