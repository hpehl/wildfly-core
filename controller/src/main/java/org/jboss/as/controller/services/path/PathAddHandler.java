/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.jboss.as.controller.services.path;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.services.path.PathResourceDefinition.PATH_SPECIFIED;
import static org.jboss.as.controller.services.path.PathResourceDefinition.RELATIVE_TO;

import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.controller.registry.Resource;
import org.jboss.as.controller.services.path.PathManager.Event;
import org.jboss.as.controller.services.path.PathManagerService.PathEventContextImpl;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceTarget;

/**
 * Handler for the path resource add operation.
 *
 * @author Brian Stansberry (c) 2011 Red Hat Inc.
 */
public class PathAddHandler implements OperationStepHandler {

    public static final String OPERATION_NAME = ADD;

    public static ModelNode getAddPathOperation(PathAddress address, ModelNode path, ModelNode relativeTo) {
        ModelNode op = Util.createAddOperation(address);
        if (path.isDefined()) {
            op.get(PATH_SPECIFIED.getName()).set(path);
        }
        if (relativeTo.isDefined()) {
            op.get(RELATIVE_TO.getName()).set(relativeTo);
        }
        return op;
    }

    private final SimpleAttributeDefinition pathAttribute;
    private final PathManagerService pathManager;
    private final boolean services;

    /**
     * Create the PathAddHandler
     */
    protected PathAddHandler(final PathManagerService pathManager, final boolean services, final SimpleAttributeDefinition pathAttribute) {
        this.pathManager = pathManager;
        this.services = services;
        this.pathAttribute = pathAttribute;
    }

    static PathAddHandler createNamedInstance(final PathManagerService pathManager) {
        return new PathAddHandler(pathManager, false, PathResourceDefinition.PATH_NAMED);
    }

    static PathAddHandler createSpecifiedInstance(final PathManagerService pathManager) {
        return new PathAddHandler(pathManager, true, PathResourceDefinition.PATH_SPECIFIED);
    }

    static PathAddHandler createSpecifiedNoServicesInstance(final PathManagerService pathManager) {
        return new PathAddHandler(pathManager, false, PathResourceDefinition.PATH_SPECIFIED);
    }


    /** {@inheritDoc */
    public void execute(final OperationContext context, final ModelNode operation) throws OperationFailedException {
        final Resource resource = context.createResource(PathAddress.EMPTY_ADDRESS);

        final ModelNode model = resource.getModel();
        final String name = context.getCurrentAddressValue();
        pathAttribute.validateAndSet(operation, model);
        RELATIVE_TO.validateAndSet(operation, model);



        if (services) {
            final String path = getPathValue(context, PATH_SPECIFIED, model);
            final String relativeTo = getPathValue(context, RELATIVE_TO, model);
            final PathEventContextImpl pathEventContext = pathManager.checkRestartRequired(context, name, Event.ADDED);

            if (pathEventContext.isInstallServices()) {
                //Add service to the path manager
                pathManager.addPathEntry(name, path, relativeTo, false);
            }

            context.addStep(new OperationStepHandler() {
                public void execute(final OperationContext context, final ModelNode operation) throws OperationFailedException {
                    final ServiceController<?> legacyService;
                    if (pathEventContext.isInstallServices()) {

                        //Add the legacy services
                        final ServiceTarget target = context.getServiceTarget();
                        if (relativeTo == null) {
                            legacyService = pathManager.addAbsolutePathService(target, name, path);
                        } else {
                            legacyService = pathManager.addRelativePathService(target, name, path, false, relativeTo);
                        }
                    } else {
                        legacyService = null;
                    }

                    context.completeStep(new OperationContext.RollbackHandler() {
                        public void handleRollback(OperationContext context, ModelNode operation) {
                            pathManager.removePathService(context, name);
                            if (pathEventContext.isInstallServices()) {
                                if (legacyService != null) {
                                    context.removeService(legacyService.getName());
                                }
                            } else {
                                pathEventContext.revert();
                            }
                        }
                    });
                }
            }, OperationContext.Stage.RUNTIME);

            context.completeStep(new OperationContext.RollbackHandler() {
                public void handleRollback(OperationContext context, ModelNode operation) {
                    if (pathEventContext.isInstallServices()) {
                        try {
                            pathManager.removePathEntry(name, false);
                        } catch (OperationFailedException e) {
                            //Should not happen since 'false' passed in for the check parameter
                            throw new RuntimeException(e);
                        }
                    }
                }
            });
        } else {
            context.completeStep(OperationContext.RollbackHandler.NOOP_ROLLBACK_HANDLER);
        }

    }

    static String getPathValue(OperationContext context, SimpleAttributeDefinition def, ModelNode model) throws OperationFailedException {
        final ModelNode resolved = def.resolveModelAttribute(context, model);
        return resolved.isDefined() ? resolved.asString() : null;
    }
}
