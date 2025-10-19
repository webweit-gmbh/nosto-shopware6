<?php

declare(strict_types=1);

namespace Nosto\NostoIntegration\Model\Operation;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Nosto\NostoIntegration\Async\CategorySyncMessage;
use Nosto\NostoIntegration\Async\EntityChangelogSyncMessage;
use Nosto\NostoIntegration\Async\EventsWriter;
use Nosto\NostoIntegration\Async\MarketingPermissionSyncMessage;
use Nosto\NostoIntegration\Async\OrderSyncMessage;
use Nosto\NostoIntegration\Async\ProductSyncMessage;
use Nosto\NostoIntegration\Entity\Changelog\ChangelogDefinition;
use Nosto\Scheduler\Model\Job\{GeneratingHandlerInterface, JobHandlerInterface, JobResult, Message\InfoMessage};
use Nosto\Scheduler\Model\JobScheduler;
use Shopware\Core\Content\Product\ProductDefinition;
use Shopware\Core\Framework\Context;
use Shopware\Core\Framework\Uuid\Uuid;

class EntityChangelogSyncHandler implements JobHandlerInterface, GeneratingHandlerInterface
{
    public const HANDLER_CODE = 'nosto-integration-entity-changelog-sync';

    private const BATCH_SIZE = 100;

    public function __construct(
        private readonly Connection $connection,
        private readonly JobScheduler $jobScheduler,
    ) {
    }

    /**
     * @param EntityChangelogSyncMessage $message
     */
    public function execute(object $message): JobResult
    {
        $result = new JobResult();
        $this->processMarketingPermissionEvents($message->getContext(), $result, $message->getJobId());
        $this->processNewOrderEvents($message->getContext(), $result, $message->getJobId());
        $this->processUpdatedOrderEvents($message->getContext(), $result, $message->getJobId());
        $this->processProductEvents($message->getContext(), $result, $message->getJobId());
        $this->processCategoryEvents($message->getContext(), $result, $message->getJobId());

        return $result;
    }

    private function processMarketingPermissionEvents(Context $context, JobResult $result, string $parentJobId): void
    {
        $type = EventsWriter::NEWSLETTER_ENTITY_NAME;
        $this->processEventBatches($type, function (array $subscriberIds) use (
            $parentJobId,
            $result,
            $context
        ): void {
            $jobMessage = new MarketingPermissionSyncMessage(Uuid::randomHex(), $parentJobId, $subscriberIds, $context);
            $this->jobScheduler->schedule($jobMessage);
            $result->addMessage(new InfoMessage(
                sprintf(
                    'Job with payload of %s marketing permission updates has been scheduled.',
                    count($subscriberIds),
                ),
            ));
        });
    }

    private function processEventBatches(string $entityType, callable $processCallback): void
    {
        $query = $this->connection->createQueryBuilder()
            ->select(
                'LOWER(HEX(entity_id)) as entityId',
                'MIN(product_number) as productNumber',
            )
            ->from(ChangelogDefinition::ENTITY_NAME)
            ->where('entity_type = :entityType')
            ->setParameter('entityType', $entityType)
            ->orderBy('created_at', 'ASC')
            ->groupBy('entity_id')
            ->setMaxResults(self::BATCH_SIZE);

        do {
            $ids = [];

            foreach ($query->executeQuery()->fetchAllAssociative() as $row) {
                $entityId = $row['entityId'] ?? null;
                if (!is_string($entityId)) {
                    continue;
                }

                if ($entityType !== ProductDefinition::ENTITY_NAME) {
                    $ids[$entityId] = $entityId;
                    continue;
                }

                $ids[$entityId] = $row['productNumber'] ?? $entityId;
            }

            if (empty($ids)) {
                break;
            }

            $processCallback($ids);

            $this->connection->createQueryBuilder()
                ->delete(ChangelogDefinition::ENTITY_NAME)
                ->where('entity_type = :entityType')
                ->andWhere('entity_id IN (:ids)')
                ->setParameter('entityType', $entityType)
                ->setParameter('ids', Uuid::fromHexToBytesList(array_keys($ids)), ArrayParameterType::BINARY)
                ->executeStatement();
        } while (!empty($ids));
    }

    private function processNewOrderEvents(Context $context, JobResult $result, string $parentJobId): void
    {
        $type = EventsWriter::ORDER_ENTITY_PLACED_NAME;
        $this->processEventBatches($type, function (array $orderIds) use (
            $parentJobId,
            $result,
            $context
        ): void {
            $jobMessage = new OrderSyncMessage(
                Uuid::randomHex(),
                $parentJobId,
                $orderIds,
                [],
                $context,
                'New Order Sync Operation',
            );
            $this->jobScheduler->schedule($jobMessage);
            $result->addMessage(new InfoMessage(
                sprintf('Job with payload of %s new orders has been scheduled.', count($orderIds)),
            ));
        });
    }

    private function processUpdatedOrderEvents(Context $context, JobResult $result, string $parentJobId): void
    {
        $type = EventsWriter::ORDER_ENTITY_UPDATED_NAME;
        $this->processEventBatches($type, function (array $orderIds) use (
            $parentJobId,
            $result,
            $context
        ): void {
            $jobMessage = new OrderSyncMessage(
                Uuid::randomHex(),
                $parentJobId,
                [],
                $orderIds,
                $context,
                'Updated Order Sync Operation',
            );
            $this->jobScheduler->schedule($jobMessage);
            $result->addMessage(new InfoMessage(
                sprintf('Job with payload of %s updated orders has been scheduled.', count($orderIds)),
            ));
        });
    }

    private function processProductEvents(Context $context, JobResult $result, string $parentJobId): void
    {
        $type = EventsWriter::PRODUCT_ENTITY_NAME;
        $this->processEventBatches($type, function (array $productIds) use (
            $parentJobId,
            $result,
            $context
        ): void {
            $jobMessage = new ProductSyncMessage(Uuid::randomHex(), $parentJobId, $productIds, $context);
            $this->jobScheduler->schedule($jobMessage);
            $result->addMessage(new InfoMessage(
                sprintf('Job with payload of %s updated products has been scheduled.', count($productIds)),
            ));
        });
    }

    private function processCategoryEvents(Context $context, JobResult $result, string $parentJobId): void
    {
        $type = EventsWriter::CATEGORY_ENTITY_NAME;
        $this->processEventBatches($type, function (array $categoryIds) use (
            $parentJobId,
            $result,
            $context
        ): void {
            $jobMessage = new CategorySyncMessage(Uuid::randomHex(), $parentJobId, $categoryIds, $context);
            $this->jobScheduler->schedule($jobMessage);
            $result->addMessage(new InfoMessage(
                sprintf('Job with payload of %s updated categories has been scheduled.', count($categoryIds)),
            ));
        });
    }
}
