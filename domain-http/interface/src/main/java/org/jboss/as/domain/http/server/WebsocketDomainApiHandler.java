/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2010, Red Hat, Inc., and individual contributors
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
package org.jboss.as.domain.http.server;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.undertow.websockets.WebSocketConnectionCallback;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.StreamSourceFrameChannel;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import io.undertow.websockets.spi.WebSocketHttpExchange;
import org.jboss.as.controller.ModelController;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.client.OperationBuilder;
import org.jboss.as.controller.client.OperationMessageHandler;
import org.jboss.dmr.ModelNode;
import org.xnio.Buffers;

/**
 * @author Harald Pehl
 */
public class WebSocketDomainApiHandler implements WebSocketConnectionCallback {

    private class ModelNodeListener extends AbstractReceiveListener {

        @Override
        protected void onError(final WebSocketChannel channel, final Throwable error) {
            // TODO Error handling
            super.onError(channel, error);
        }

        @Override
        protected void onBinary(final WebSocketChannel webSocketChannel, final StreamSourceFrameChannel messageChannel)
                throws IOException {
            ModelNode operation = new ModelNode();
            // TODO Read binary data and turn that into a ModelNode: operation.readExternal(...);

            final boolean sendPreparedResponse = sendPreparedResponse(operation);
            final ModelController.OperationTransactionControl control = sendPreparedResponse ? new ModelController.OperationTransactionControl() {
                @Override
                public void operationPrepared(final ModelController.OperationTransaction transaction,
                        final ModelNode result) {
                    transaction.commit();
                    result.get(OUTCOME).set(SUCCESS);
                    result.get(RESULT);
                    // TODO Replace with WS equivalent
                    // callback.sendResponse(result);
                }
            } : ModelController.OperationTransactionControl.COMMIT;
            ModelNode resultNode = modelController.execute(operation, OperationMessageHandler.logging, control,
                    new OperationBuilder(operation).build());

            // TODO Turn result into binary data
            ByteBuffer response = Buffers.EMPTY_BYTE_BUFFER;
            WebSockets.sendBinary(response, webSocketChannel, null);
        }

        /**
         * Determine whether the prepared response should be sent, before the operation completed. This is needed in order
         * that operations like :reload() can be executed without causing communication failures.
         *
         * @param operation the operation to be executed
         *
         * @return {@code true} if the prepared result should be sent, {@code false} otherwise
         */
        private boolean sendPreparedResponse(final ModelNode operation) {
            final PathAddress address = PathAddress.pathAddress(operation.get(OP_ADDR));
            final String op = operation.get(OP).asString();
            final int size = address.size();
            if (size == 0) {
                if (op.equals("reload")) {
                    return true;
                } else if (op.equals(COMPOSITE)) {
                    return false;
                } else {
                    return false;
                }
            } else if (size == 1) {
                if (address.getLastElement().getKey().equals(HOST)) {
                    return op.equals("reload");
                }
            }
            return false;
        }
    }


    static String PATH = "/ws-management";

    private final ModelController modelController;

    public WebSocketDomainApiHandler(final ModelController modelController) {
        this.modelController = modelController;
    }

    @Override
    public void onConnect(final WebSocketHttpExchange exchange, final WebSocketChannel channel) {
        // TODO Verify origin header (CORS)
        channel.getReceiveSetter().set(new ModelNodeListener());
    }
}
