/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.as.controller.operations.global;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PrimitiveListAttributeDefinition;
import org.jboss.as.controller.SimpleMapAttributeDefinition;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.descriptions.common.ControllerResolver;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.dmr.Property;

import java.util.ArrayList;
import java.util.List;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

/**
 *
 * @author Heiko Braun (c) 2011 Red Hat Inc.
 */
public final class MapReduceOperationHandler extends GlobalOperationHandlers.AbstractMultiTargetHandler {

    public static final MapReduceOperationHandler INSTANCE = new MapReduceOperationHandler();

    private static final AttributeDefinition FILTER_ATT = new SimpleMapAttributeDefinition.Builder(ModelDescriptionConstants.FILTER, ModelType.STRING, true)
            .build();

    private static final AttributeDefinition CONJUNCT_ATT = new SimpleMapAttributeDefinition.Builder(ModelDescriptionConstants.CONJUNCT, ModelType.BOOLEAN, true)
                .build();

    private static final AttributeDefinition REDUCE_ATT = new PrimitiveListAttributeDefinition.Builder(ModelDescriptionConstants.REDUCE, ModelType.STRING)
            .setAllowNull(true)
            .build();


    public static final OperationDefinition DEFINITION = new SimpleOperationDefinitionBuilder(ModelDescriptionConstants.MAP_REDUCE, ControllerResolver.getResolver("global"))
            .addParameter(FILTER_ATT)
            .addParameter(CONJUNCT_ATT)
            .addParameter(REDUCE_ATT)
            .setReplyType(ModelType.OBJECT)
            .build();

    private MapReduceOperationHandler() {

    }

    @Override
    void doExecute(final OperationContext parentContext, ModelNode operation, FilteredData filteredData) throws OperationFailedException {
        FILTER_ATT.validateOperation(operation);
        CONJUNCT_ATT.validateOperation(operation);
        REDUCE_ATT.validateOperation(operation);

        ManagementResourceRegistration mrr = parentContext.getResourceRegistrationForUpdate();
        final OperationStepHandler readResourceHandler = mrr.getOperationHandler(
                PathAddress.EMPTY_ADDRESS,
                ModelDescriptionConstants.READ_RESOURCE_OPERATION
        );

        final ModelNode readResourceOp = new ModelNode();
        readResourceOp.get(ADDRESS).set(operation.get(ADDRESS));
        readResourceOp.get(OP).set(READ_RESOURCE_OPERATION);
        readResourceOp.get(INCLUDE_RUNTIME).set(true);

        // map phase
        parentContext.addStep(readResourceOp, readResourceHandler, OperationContext.Stage.MODEL);

        // filter/reduce phase
        parentContext.addStep(operation, FilterReduceHandler.INSTANCE, OperationContext.Stage.VERIFY);

        parentContext.stepCompleted();

    }

    static class FilterReduceHandler implements OperationStepHandler {

        static final FilterReduceHandler INSTANCE = new FilterReduceHandler();

        @Override
        public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {

            if(operation.hasDefined(FILTER)) {
                boolean conjunct = operation.hasDefined(CONJUNCT) ? operation.get(CONJUNCT).asBoolean() : false;
                boolean matches = matchesFilter(context.getResult(), operation.get(FILTER), conjunct);
                // if the filter doesn't match we remove it from the response
                if(!matches)
                    context.getResult().set(ModelType.UNDEFINED);
            }

            context.stepCompleted();
        }

        private static boolean matchesFilter(final ModelNode payload, final ModelNode filter, final boolean conjunct) throws OperationFailedException {
            List<Property> filterProperties = filter.asPropertyList();
            List<Boolean> matches = new ArrayList<>(filterProperties.size());

            for (Property property : filterProperties) {
                String filterName = property.getName();
                ModelNode filterValue = property.getValue();

                if (payload.get(filterName).isDefined()) {

                    // attribute is defined on this resource, evaluate filter ...
                    if(payload.get(filterName).equals(filterValue)) {
                        matches.add(payload.get(filterName).equals(filterValue));
                    }
                } else {
                    // Unmatched filter attributes are treated according to the conjunct/disjunct semantics
                    return conjunct ? false : true;
                }
            }

            if (conjunct) {
                // all matches must be true
                for (boolean match : matches) {
                    if (!match) {
                        return false;
                    }
                }
                return true;

            } else {
                // at least one match must be true
                for (Boolean match : matches) {
                    if (match) {
                        return true;
                    }
                }
                return false;
            }
        }

      /*  private void reduce(final ModelNode result, final ModelNode attributes) throws OperationFailedException {
            // make sure all attributes are defined
            List<String> names = new ArrayList<>();
            List<String> undefined = new ArrayList<>();
            for (ModelNode attribute : attributes.asList()) {
                String name = attribute.asString();
                ModelNode value = result.get(name);
                if (value.isDefined()) {
                    names.add(name);
                } else {
                    undefined.add("\"" + name + "\"");
                }
            }

            if (!undefined.isEmpty()) {
                throw  new OperationFailedException("Attributes " + names + " not defined on this resource");

            } else {
                ModelNode reduced = new ModelNode();
                for (String name : names) {
                    ModelNode value = result.get(name);
                    reduced.get(name).set(value);
                }
            }
        }*/
    }
}
